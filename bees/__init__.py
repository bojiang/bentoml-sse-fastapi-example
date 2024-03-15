import asyncio
import collections
import json
import os
import time

import aiohttp
import psutil
from pyformance.registry import MetricsRegistry

import curlparser

MAX_COLD_START_TIME = 20
PID = os.getpid()
CURRENT_PROC = psutil.Process(PID)


def _make_metrics_registry() -> MetricsRegistry:
    registry = MetricsRegistry()
    registry.counter("user")
    registry.counter("request.total")
    registry.counter("request.error")
    registry.gauge("request.active")
    registry.histogram("response.latency")
    return registry


METRICS = collections.defaultdict(_make_metrics_registry)
DATAS = collections.defaultdict(list)
NOTI_HAS_DATA = collections.defaultdict(asyncio.Event)
NOTI_RUNNING = collections.defaultdict(asyncio.Event)
NOTI_STOPPING = collections.defaultdict(asyncio.Event)


def _get_status(request_id: str):
    if NOTI_STOPPING[request_id].is_set():
        return "stopped"
    if NOTI_RUNNING[request_id].is_set():
        return "running"
    else:
        return "paused"


async def _data_collector_loop(request_id: str, interval: int = 2):
    try:
        last_total = 0
        last_total_errors = 0
        start_time = time.time()
        while True:
            if not NOTI_RUNNING[request_id].is_set():  # paused
                METRICS[request_id] = _make_metrics_registry()
                last_total = 0
                last_total_errors = 0
            registry = METRICS[request_id]
            now = int((time.time() - start_time) * 100) / 100

            total = registry.counter("request.total").get_count()
            DATAS[request_id].append(
                {
                    "plot": "throughput",
                    "data": {
                        "x": [[now]],
                        "y": [[(total - last_total) / interval]],
                    },
                    "trace": 0,
                    "operation": "extend",
                }
            )
            last_total = total

            total = registry.counter("request.error").get_count()
            DATAS[request_id].append(
                {
                    "plot": "throughput",
                    "data": {
                        "x": [[now]],
                        "y": [[(total - last_total_errors) / interval]],
                    },
                    "trace": 1,
                    "operation": "extend",
                }
            )
            last_total_errors = total

            if registry.counter("request.total").get_count() > 0:
                # latency metrics
                DATAS[request_id].append(
                    {
                        "plot": "latency",
                        "data": {
                            "x": [[now]],
                            "y": [[registry.histogram("response.latency").get_max()]],
                        },
                        "trace": 0,
                        "operation": "extend",
                    }
                )
                DATAS[request_id].append(
                    {
                        "plot": "latency",
                        "data": {
                            "x": [[now]],
                            "y": [
                                [
                                    registry.histogram("response.latency")
                                    .get_snapshot()
                                    .get_99th_percentile()
                                ]
                            ],
                        },
                        "trace": 1,
                        "operation": "extend",
                    }
                )
                DATAS[request_id].append(
                    {
                        "plot": "latency",
                        "data": {
                            "x": [[now]],
                            "y": [
                                [
                                    registry.histogram("response.latency")
                                    .get_snapshot()
                                    .get_median()
                                ]
                            ],
                        },
                        "trace": 2,
                        "operation": "extend",
                    }
                )

            DATAS[request_id].append(
                {
                    "plot": "system",
                    "data": [
                        [_get_status(request_id)],
                        [registry.counter("user").get_count()],
                        [registry.counter("request.total").get_count()],
                        [registry.counter("request.error").get_count()],
                        [registry.histogram("response.latency").get_mean()],
                        [f"{CURRENT_PROC.cpu_percent(interval=None)}%"],
                    ],
                    "trace": 0,
                    "operation": "replace",
                }
            )

            # error metrics
            error_metrics = tuple(
                k for k in registry.dump_metrics() if k.startswith("error.")
            )
            error_infos = [
                (k.split(".", maxsplit=1)[1], registry.counter(k).get_count())
                for k in error_metrics
            ]
            error_infos.sort(key=lambda x: x[1], reverse=True)
            DATAS[request_id].append(
                {
                    "plot": "error",
                    "data": [
                        [k.split(".", maxsplit=1)[0] for k, _ in error_infos],
                        [k.split(".", maxsplit=1)[1] for k, _ in error_infos],
                        [v for _, v in error_infos],
                    ],
                    "trace": 0,
                    "operation": "replace",
                }
            )
            NOTI_HAS_DATA[request_id].set()
            if NOTI_STOPPING[request_id].is_set():  # stopped
                return
            if not NOTI_RUNNING[request_id].is_set():  # paused
                await NOTI_RUNNING[request_id].wait()  # wait for resume
            await asyncio.sleep(interval)
    except Exception as e:
        DATAS[request_id].append(
            {
                "plot": "error",
                "data": [
                    [f"Bees internal error: {type(e).__name__}"],
                    [str(e)],
                    [1],
                ],
                "trace": 0,
                "operation": "replace",
            }
        )
    finally:
        DATAS[request_id].append(None)
        NOTI_HAS_DATA[request_id].set()
        return


async def _stream_chart_data(request_id: str):
    cursor = 0
    while True:
        if cursor < len(DATAS[request_id]):
            data = DATAS[request_id][cursor]
            if data is None:
                yield b"event: close\ndata: \n\n"
                return
            else:
                yield f"data: {json.dumps(data)}\n\n".encode("utf-8")
                cursor += 1
        else:
            NOTI_HAS_DATA[request_id].clear()
            await NOTI_HAS_DATA[request_id].wait()


async def _user_loop(
    request_id: str,
    request_info: curlparser.parser.ParsedCommand,
    timeout_override: int | None,
    start_delay: float = 0,
) -> None:
    await asyncio.sleep(start_delay)
    dummy_cookie_jar = aiohttp.DummyCookieJar()
    METRICS[request_id].counter("user").inc()
    timeout = timeout_override or request_info.max_time
    while True:
        if NOTI_STOPPING[request_id].is_set():  # stopped
            METRICS[request_id].counter("user").dec()
            return
        if not NOTI_RUNNING[request_id].is_set():  # paused
            METRICS[request_id].counter("user").dec()
            await NOTI_RUNNING[request_id].wait()
            await asyncio.sleep(start_delay)
            METRICS[request_id].counter("user").inc()
        try:
            METRICS[request_id].counter("request.active").inc()
            async with aiohttp.ClientSession(
                cookie_jar=dummy_cookie_jar,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as session:
                now = time.time()
                async with session.request(
                    method=request_info.method,
                    url=request_info.url,
                    headers=request_info.headers,
                    data=request_info.data,
                    cookies=request_info.cookies,
                    timeout=timeout,
                ) as response:
                    content = await response.read()
                    abstract = content.decode()[:50]
                METRICS[request_id].histogram("response.latency").add(time.time() - now)
                METRICS[request_id].counter("request.total").inc()
                if response.status >= 400 and response.status < 600:
                    METRICS[request_id].counter("request.error").inc()
                    METRICS[request_id].counter(
                        f"error.{response.status}.{abstract}"
                    ).inc()
        except Exception as e:
            abstract = str(e)[:50]
            METRICS[request_id].counter(f"error.{type(e).__name__}.{abstract}").inc()
            METRICS[request_id].counter("request.error").inc()
        finally:
            METRICS[request_id].counter("request.active").dec()


async def _benchmark_controller(
    result_id: str,
    code,
    users: int | None,
    duration: int,
    timeout_override: int | None = None,
    collector_interval: int = 2,
):
    if result_id in METRICS:
        return
    if users is None:
        users = 10
    parsed = curlparser.parse(code)
    cold_start_time = min(duration / 3, MAX_COLD_START_TIME)

    tasks: list[asyncio.Task] = []
    try:
        tasks.append(
            asyncio.create_task(
                _data_collector_loop(
                    result_id,
                    collector_interval,
                ),
                name=f"collector-{result_id}",
            )
        )
        for i in range(users):
            tasks.append(
                asyncio.create_task(
                    _user_loop(
                        result_id,
                        parsed,
                        timeout_override,
                        start_delay=cold_start_time / users * i,
                    ),
                    name=f"user-{result_id}-{i}",
                ),
            )
        NOTI_RUNNING[result_id].set()
        while duration >= 0:
            if NOTI_STOPPING[result_id].is_set():
                break
            if not NOTI_RUNNING[result_id].is_set():
                await NOTI_RUNNING[result_id].wait()
            else:
                await asyncio.sleep(1)
                duration -= 1
        else:
            NOTI_STOPPING[result_id].set()
    finally:
        for task in tasks[::-1]:
            await asyncio.wait_for(
                task, timeout=(timeout_override or parsed.max_time or 10) + 1
            )

        await asyncio.sleep(1800)

        # clean
        if result_id in METRICS:
            METRICS.pop(result_id)
        if result_id in METRICS:
            METRICS.pop(result_id)
        if result_id in DATAS:
            DATAS.pop(result_id)
        if result_id in NOTI_HAS_DATA:
            NOTI_HAS_DATA.pop(result_id)
        if result_id in NOTI_RUNNING:
            NOTI_RUNNING.pop(result_id)
        if result_id in NOTI_STOPPING:
            NOTI_STOPPING.pop(result_id)


TEMPLATE_RESULT = """
<!DOCTYPE html>
<html>
<head>
    <title>Bees: Benchmark Result</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>
<button onclick="fetch('/pause_bento_benchmark', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({result_id: '{{ chart_id }}'})})">Pause</button>
<button onclick="fetch('/resume_bento_benchmark', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({result_id: '{{ chart_id }}'})})">Resume</button>
<button onclick="fetch('/stop_bento_benchmark', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({result_id: '{{ chart_id }}'})})">Stop</button>

{% for plot in plots %}
    <div id="{{ plot.name }}"></div>
{% endfor %}

<script>
    var plots = {{ plots | tojson }};
    var plotMap = {};
    for (var i = 0; i < plots.length; i++) {
        var plot = plots[i];
        Plotly.newPlot(plot.name, plot.traces, plot.layout);
        plotMap[plot.name] = plot;
    }

    var source = new EventSource('/chart/{{ chart_id }}/stream');
    source.onmessage = function(event) {
        var data = JSON.parse(event.data);
        var plot = plotMap[data.plot];
        if (plot.traces[data.trace].type === 'table') {
            if (data.operation === 'extend') {
                for (var i = 0; i < data.data.length; i++) {
                    plot.traces[data.trace].cells.values[i].push(data.data[i][0]);
                }
            } else {
                plot.traces[data.trace].cells.values = data.data;
            }
            Plotly.react(plot.name, plot.traces, plot.layout);
        } else {
            if (data.operation === 'replace') {
                plot.traces[data.trace].x = data.data.x[0];
                plot.traces[data.trace].y = data.data.y[0];
                Plotly.react(plot.name, plot.traces, plot.layout);
            } else {
                Plotly.extendTraces(data.plot, data.data, [data.trace]);
            }
        }
    };
    source.addEventListener('close', function(event) {
        source.close();
    });

</script>

</body>
</html>
        """


TEMPLATE_INDEX = r"""
        <html>
        <head>
            <script>
                function submitForm(event) {
                    event.preventDefault();
                    var form = document.getElementById("benchmark-form");
                    var users = form.elements["users"].value;
                    var duration = form.elements["duration"].value;
                    var code = form.elements["code"].value;
                    var timeout = form.elements["timeout"].value;
                    var interval = form.elements["interval"].value;
                    if (users < 1 || duration < 1) {
                        alert("Users and duration must be greater than 0");
                        return;
                    }
                    // fetch the result chart page and jump to it
                    fetch("/start_bento_benchmark", {
                        method: "POST",
                        headers: {
                            "Content-Type": "application/json",
                        },
                        body: JSON.stringify({
                            code: code,
                            users: users,
                            duration: duration,
                            timeout: timeout,
                            interval: interval,
                        }),
                    }).then(response => response.json()).then(data => {
                        window.location.href = data.result;
                    });
                }
            </script>
        </head>
        <body>
        <h1>Bees</h1>
        <p>A benchmark service powered by BentoML.</p>
        <p>Submit a curl command (could be copied from playground) and the number of users and duration, then jump to the result chart page.</p>
        <form id="benchmark-form" onsubmit="submitForm(event)">
            <label for="code">Curl Command:</label><br>
            <textarea id="code" name="code" rows="4" cols="50">curl https://httpbin.org</textarea><br><br>
            <label for="users">Users:</label><br>
            <input type="number" id="users" name="users" value="10"><br>
            <label for="duration">Test Duration:(s)</label><br>
            <input type="number" id="duration" name="duration" value="60"><br>
            <label for="timeout">Timeout:(s)</label><br>
            <input type="number" id="timeout" name="timeout" value="60"><br>
            <label for="interval">Collector Interval:(s)</label><br>
            <input type="number" id="interval" name="interval" value="2"><br><br>
            <input type="submit" value="Submit">
        </form>
        </body>
        </html>
"""

PLOTS_RESULT = [
    {
        "name": "system",
        "traces": [
            {
                "type": "table",
                "header": {
                    "values": [
                        "Status",
                        "Active Users",
                        "Requests",
                        "Errors",
                        "Average Latency(s)",
                        "Client CPU Usage",
                    ],
                    "align": "center",
                    "line": {"width": 1, "color": "black"},
                    "fill": {"color": "grey"},
                    "font": {"family": "Arial", "size": 12, "color": "white"},
                },
                "cells": {
                    "values": [[0], [0], [0], [0], ["0%"]],
                    "align": "center",
                    "line": {"color": "black", "width": 1},
                    "fill": {"color": ["white", "white", "white", "white"]},
                    "font": {"family": "Arial", "size": 11, "color": ["black"]},
                },
            }
        ],
        "layout": {
            "title": "System Status",
            "height": 250,
        },
    },
    {
        "name": "throughput",
        "traces": [
            {
                "x": [],
                "y": [],
                "mode": "lines+markers",
                "type": "scatter",
                "fill": "tozeroy",
                "name": "success",
            },
            {
                "x": [],
                "y": [],
                "mode": "lines+markers",
                "type": "scatter",
                "fill": "tozeroy",
                "line": {"color": "red"},
                "name": "error",
            },
        ],
        "layout": {
            "title": "Throughput",
            "xaxis": {"title": "time(s)"},
            "yaxis": {"title": "requests/s"},
        },
    },
    {
        "name": "latency",
        "traces": [
            {
                "x": [],
                "y": [],
                "mode": "lines+markers",
                "type": "scatter",
                "fill": "tozeroy",
                "name": "P100",
                "line": {"color": "red"},
            },
            {
                "x": [],
                "y": [],
                "mode": "lines+markers",
                "type": "scatter",
                "fill": "tozeroy",
                "line": {"color": "orange"},
                "name": "P99",
            },
            {
                "x": [],
                "y": [],
                "mode": "lines+markers",
                "type": "scatter",
                "fill": "tozeroy",
                "line": {"color": "green"},
                "name": "P50",
            },
        ],
        "layout": {
            "title": "Latency(s)",
            "xaxis": {"title": "time(s)"},
            "yaxis": {"title": "latency(s)"},
        },
    },
    {
        "name": "error",
        "traces": [
            {
                "type": "table",
                "header": {
                    "values": ["error", "messages", "count"],
                    "align": "center",
                    "line": {"width": 1, "color": "black"},
                    "fill": {"color": "grey"},
                    "font": {"family": "Arial", "size": 12, "color": "white"},
                },
                "cells": {
                    "values": [],
                    "align": "center",
                    "line": {"color": "black", "width": 1},
                    "fill": {"color": ["white", "white", "white", "white"]},
                    "font": {"family": "Arial", "size": 11, "color": ["black"]},
                },
            }
        ],
        "layout": {
            "title": "Error",
        },
    },
]
