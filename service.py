import asyncio
import collections
import json
import math
import time
import uuid

import aiohttp
import bentoml
import jinja2
import starlette.applications
import starlette.responses
from pyformance.registry import MetricsRegistry

import curlparser

MAX_COLD_START_TIME = 20
COLLECTION_INTERVAL = 5


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
NOTIFICATIONS = collections.defaultdict(asyncio.Event)


async def _data_collector_loop(request_id: str):
    try:
        last_total = 0
        last_total_errors = 0
        start_time = time.time()
        while True:
            registry = METRICS[request_id]
            total = registry.counter("request.total").get_count()
            DATAS[request_id].append(
                {
                    "plot": "throughput",
                    "data": {
                        "x": [[time.time() - start_time]],
                        "y": [[(total - last_total) / COLLECTION_INTERVAL]],
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
                        "x": [[time.time() - start_time]],
                        "y": [[(total - last_total_errors) / COLLECTION_INTERVAL]],
                    },
                    "trace": 1,
                    "operation": "extend",
                }
            )
            last_total_errors = total

            DATAS[request_id].append(
                {
                    "plot": "user",
                    "data": [
                        [registry.counter("user").get_count()],
                        [registry.counter("request.total").get_count()],
                        [registry.counter("request.error").get_count()],
                        [registry.histogram("response.latency").get_mean()],
                    ],
                    "trace": 0,
                    "operation": "replace",
                }
            )
            error_metrics = tuple(
                k for k in registry.dump_metrics() if k.startswith("error.")
            )
            error_infos = [
                [k.split(".", maxsplit=1)[1] for k in error_metrics],
                [registry.counter(k).get_count() for k in error_metrics],
            ]
            DATAS[request_id].append(
                {
                    "plot": "error",
                    "data": error_infos.sort(key=lambda x: x[1], reverse=True),
                    "trace": 0,
                    "operation": "replace",
                }
            )
            NOTIFICATIONS[request_id].set()
            await asyncio.sleep(COLLECTION_INTERVAL)
    finally:
        DATAS[request_id].append(None)
        NOTIFICATIONS[request_id].set()
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
            NOTIFICATIONS[request_id].clear()
            await NOTIFICATIONS[request_id].wait()


async def _user_loop(request_id: str, request_info: curlparser.parser.ParsedCommand):
    dummy_cookie_jar = aiohttp.DummyCookieJar()
    METRICS[request_id].counter("user").inc()
    while True:
        async with aiohttp.ClientSession(cookie_jar=dummy_cookie_jar) as session:
            now = time.time()
            try:
                METRICS[request_id].counter("request.active").inc()
                async with session.request(
                    method=request_info.method,
                    url=request_info.url,
                    headers=request_info.headers,
                    data=request_info.data,
                    cookies=request_info.cookies,
                    timeout=request_info.max_time,
                ) as response:
                    content = await response.read()
                    abstract = content.decode()[:20]
                METRICS[request_id].histogram("response.latency").add(time.time() - now)
                METRICS[request_id].counter("request.total").inc()
                if response.status >= 400 and response.status < 600:
                    METRICS[request_id].counter("request.error").inc()
                    METRICS[request_id].counter(
                        f"error.{response.status}.{abstract}"
                    ).inc()
            except asyncio.CancelledError:
                METRICS[request_id].counter("user").dec()
                return
            except Exception as e:
                METRICS[request_id].counter(f"error.{type(e).__name__}.{e}").inc()
                METRICS[request_id].counter("request.error").inc()
            finally:
                METRICS[request_id].counter("request.active").dec()


async def _benchmark_task(result_id: str, code, users: int | None, duration: int):
    if result_id in METRICS:
        return
    if users is None:
        users = 10
    parsed = curlparser.parse(code)
    cold_start_time = min(duration / 10, MAX_COLD_START_TIME)

    benchmark_start = time.time()

    user_tasks = []
    spawn_interval = float(cold_start_time) / users
    collector_task = asyncio.create_task(_data_collector_loop(result_id))
    for i in range(users):
        user_tasks.append(
            asyncio.create_task(_user_loop(result_id, parsed), name=f"user_{i}")
        )
        await asyncio.sleep(spawn_interval)

    await asyncio.sleep(duration - (time.time() - benchmark_start))
    for task in user_tasks:
        task.cancel()
    collector_task.cancel()

    await asyncio.sleep(1800)
    METRICS.pop(result_id)
    DATAS.pop(result_id)
    NOTIFICATIONS.pop(result_id)


@bentoml.service
class Bees:
    @bentoml.api
    async def hpa_calculator(
        self,
        current_metric: float = 100,
        target_metric: float = 90,
        current_pods: int = 1,
    ) -> int:
        return math.ceil(
            math.floor((current_pods * (current_metric / target_metric)) * 10) / 10
        )

    @bentoml.api
    async def benchmark_bento_api(
        self,
        code: str = "curl https://baidu.com",
        users: int = 10,
        duration: int = 300,
        ctx: bentoml.Context = None,  # type: ignore
    ) -> str:
        result_id = str(uuid.uuid4())
        asyncio.create_task(_benchmark_task(result_id, code, users, duration))
        ctx.response.status_code = 202
        ctx.response.headers["Content-Type"] = "text/html"
        return """
        <html>
        <head>
            <title>Accepted</title>
        </head>
        <body>
            <h1>Accepted</h1>
            <p>Your request has been accepted and is being processed.</p>
            <p>Result ID: {result_id}</p>
            <p>Result URL: <a href="/chart/{result_id}"> Check Result</a></p>
        </body>
        </html>
        """.format(
            result_id=result_id
        )


app = starlette.applications.Starlette()

TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Real-time Scatter Plot with Plotly.js</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
</head>
<body>

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


@app.route("/chart/{chart_id}")
async def chart(request):
    chart_id = request.path_params["chart_id"]
    plots = [
        {
            "name": "user",
            "traces": [
                {
                    "type": "table",
                    "header": {
                        "values": ["users", "requests", "errors", "response time(ms)"],
                        "align": "center",
                        "line": {"width": 1, "color": "black"},
                        "fill": {"color": "grey"},
                        "font": {"family": "Arial", "size": 12, "color": "white"},
                    },
                    "cells": {
                        "values": [[0], [0], [0], [0]],
                        "align": "center",
                        "line": {"color": "black", "width": 1},
                        "fill": {"color": ["white", "white", "white", "white"]},
                        "font": {"family": "Arial", "size": 11, "color": ["black"]},
                    },
                }
            ],
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
                },
                {
                    "x": [],
                    "y": [],
                    "mode": "lines+markers",
                    "type": "scatter",
                    "fill": "tozeroy",
                    "line": {"color": "red"},
                },
            ],
            "layout": {
                "title": "Throughput",
                "xaxis": {"title": "time(s)"},
                "yaxis": {"title": "requests/s"},
            },
        },
        {
            "name": "error",
            "traces": [
                {
                    "type": "table",
                    "header": {
                        "values": ["error", "count"],
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
        },
    ]
    return starlette.responses.HTMLResponse(
        jinja2.Template(TEMPLATE).render(
            chart_id=chart_id,
            plots=plots,
        )
    )


@app.route("/chart/{chart_id}/stream")
async def chart_stream(request):
    chart_id = request.path_params["chart_id"]
    return starlette.responses.StreamingResponse(
        content=_stream_chart_data(chart_id),
        media_type="text/event-stream",
    )


Bees.mount_asgi_app(app)
