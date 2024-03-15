import asyncio
import math
import uuid

import bentoml
import jinja2
import starlette.applications
import starlette.responses

from bees import (NOTI_RUNNING, NOTI_STOPPING, PLOTS_RESULT, TEMPLATE_INDEX,
                  TEMPLATE_RESULT, _benchmark_controller, _stream_chart_data)


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
    async def start_bento_benchmark(
        self,
        code: str = "curl https://httpbin.org",
        users: int = 10,
        duration: int = 60,
        timeout: int | None = None,
        interval: int = 2,
    ) -> dict:
        result_id = str(uuid.uuid4())
        asyncio.create_task(
            _benchmark_controller(
                result_id,
                code,
                users,
                duration,
                timeout,
                interval,
            )
        )
        return {
            "status": "running",
            "result": f"/chart/{result_id}",
        }

    @bentoml.api
    async def stop_bento_benchmark(self, result_id: str) -> dict:
        NOTI_STOPPING[result_id].set()
        return {"status": "stopped"}

    @bentoml.api
    async def pause_bento_benchmark(self, result_id: str) -> dict:
        if NOTI_STOPPING[result_id].is_set():
            return {"status": "stopped"}
        NOTI_RUNNING[result_id].clear()
        return {"status": "paused"}

    @bentoml.api
    async def resume_bento_benchmark(self, result_id: str) -> dict:
        if NOTI_STOPPING[result_id].is_set():
            return {"status": "stopped"}
        NOTI_RUNNING[result_id].set()
        return {"status": "running"}


app = starlette.applications.Starlette()


@app.route("/")
async def index(_):
    return starlette.responses.HTMLResponse(jinja2.Template(TEMPLATE_INDEX).render())


@app.route("/chart/{chart_id}")
async def chart(request):
    chart_id = request.path_params["chart_id"]
    return starlette.responses.HTMLResponse(
        jinja2.Template(TEMPLATE_RESULT).render(
            chart_id=chart_id,
            plots=PLOTS_RESULT,
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
