import asyncio
import collections
import json
import math
import sqlite3
import time
import typing as t
import uuid

import aiohttp
import bentoml
import jinja2
import starlette.applications
import starlette.responses

import curlparser

MAX_COLD_START_TIME = 20
COLLECTION_INTERVAL = 5

"""
Time: 11
Active Users: 1
Request/s: 0.0
Total Requests: 1
Active Requests: 1
Response Tokens/s: 0.0
Status: defaultdict(<class 'int'>, {})
"""


# 创建一个内存中的 SQLite 数据库
conn = sqlite3.connect(":memory:")
cursor = conn.cursor()


METRICS = {
    "user_active": {
        "type": "Gauge",
        "labels": ["time"],
    },
    "response": {
        "type": "Counter",
        "labels": ["time", "status"],
    },
    "exception": {
        "type": "Counter",
        "labels": ["time", "exception"],
    },
    "request_active": {
        "type": "Gauge",
        "labels": ["time"],
    },
    "client_cpu": {
        "type": "Gauge",
        "labels": ["time"],
    },
}

cursor.execute("CREATE TABLE example (id INTEGER, name TEXT)")


class AverageCounter:
    def __init__(self):
        self.value: float = 0
        self.total = 0

    def __iadd__(self, other: float):
        self.value = other / (self.total + 1) + self.value * (
            self.total / (self.total + 1)
        )
        self.total += 1


def task_factory():
    return {
        "Active Users": collections.defaultdict(int),
        "Active Requests": collections.defaultdict(int),
        "Latency": collections.defaultdict(AverageCounter),
        "2xx": collections.defaultdict(int),
        "3xx": collections.defaultdict(int),
        "4xx": collections.defaultdict(int),
        "5xx": collections.defaultdict(int),
        "Timeouts": collections.defaultdict(int),
        "Unkown Errors": collections.defaultdict(int),
    }


QUEUE = collections.defaultdict(task_factory)


async def user_loop(request_id: str, request_info: curlparser.parser.ParsedCommand):
    """ """
    dummy_cookie_jar = aiohttp.DummyCookieJar()
    while True:
        async with aiohttp.ClientSession(cookie_jar=dummy_cookie_jar) as session:
            now = time.time()
            time_id = math.floor(now / COLLECTION_INTERVAL) * COLLECTION_INTERVAL
            try:
                async with session.request(
                    method=request_info.method,
                    url=request_info.url,
                    headers=request_info.headers,
                    data=request_info.data,
                    cookies=request_info.cookies,
                    timeout=request_info.max_time,
                ) as response:
                    await response.read()
            except aiohttp.ClientError:
                pass
                QUEUE[request_id]["Timeouts"][time_id] += 1

            QUEUE[request_id]["Finished Requests"][time_id] += 1
            latency = time.time() - now
            QUEUE[request_id]["Latency"][time_id] += latency
            if response.status // 100 == 2:
                QUEUE[request_id]["2xx"][time_id] += 1
            elif response.status // 100 == 3:
                QUEUE[request_id]["3xx"][time_id] += 1
            elif response.status // 100 == 4:
                QUEUE[request_id]["4xx"][time_id] += 1


async def benchmark_task(result_id: str, code, users: int | None, duration: int):
    if result_id in QUEUE:
        return
    parsed = curlparser.parse(code)
    parsed.method
    cold_start_time = min(duration / 10, MAX_COLD_START_TIME)


@bentoml.service
class BentoSwissArmyKnife:
    @bentoml.api
    async def hpa_calculator(
        self,
        current_metric: float,
        target_metric: float,
        current_pods: int = 1,
    ) -> int:
        return math.ceil(
            math.floor((current_pods * (current_metric / target_metric)) * 10) / 10
        )

    @bentoml.api
    async def benchmark_bento_api(
        self,
        code: str,
        users: int | None = None,
        duration: int = 300,
    ) -> dict:
        result_id = str(uuid.uuid4())
        return {
            "result_id": "123",
            "result_url": "/chart/123",
        }


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

    trace = {
        "x": [1],
        "y": [2],
        "mode": "lines+markers",
        "type": "scatter",
    }

    layout = {
        "title": "Throughput",
        "xaxis": {"title": "time(s)"},
        "yaxis": {"title": "request/s"},
    }

    table = {
        "type": "table",
        "header": {
            "values": ["requests", "users", "errors", "response time(ms)"],
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

    plot = {
        "name": "plot",
        "traces": [trace],
        "layout": layout,
    }
    plot2 = {
        "name": "plot2",
        "traces": [table],
    }
    return starlette.responses.HTMLResponse(
        jinja2.Template(TEMPLATE).render(
            chart_id=chart_id,
            plots=[plot, plot2],
        )
    )


async def generate_data():
    for i in range(10):
        await asyncio.sleep(2)
        data = {
            "plot": "plot",
            "data": {"x": [[i]], "y": [[i * 2]]},
            "trace": 0,
        }
        yield f"data: {json.dumps(data)}\n\n".encode("utf-8")
        data = {
            "plot": "plot2",
            "data": [[i], [i * 2], [i * 3], [i * 4]],
            "trace": 0,
        }
        yield f"data: {json.dumps(data)}\n\n".encode("utf-8")
    yield b"event: close\ndata: \n\n"


@app.route("/chart/{chart_id}/stream")
async def chart_stream(request):
    chart_id = request.path_params["chart_id"]
    return starlette.responses.StreamingResponse(
        content=generate_data(),
        media_type="text/event-stream",
    )


BentoSwissArmyKnife.mount_asgi_app(app)
