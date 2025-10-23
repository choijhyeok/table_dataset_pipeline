import os
import json
from typing import Any, Dict

from pydantic import ValidationError
import azure.functions as func
from azure.durable_functions import DurableOrchestrationClient
from application.app import app

from orchestrators.terms_parsing_orchestrator import (
    meritz_orchestrator,
    RequestBodyChecker
)

from dotenv import load_dotenv
load_dotenv()


@app.function_name(name="http_process_main_orchestrator")
@app.route(
    route="process-dynamic-main", methods=["POST"], auth_level=func.AuthLevel.ANONYMOUS
)
@app.durable_client_input(client_name="client")
async def http_process_dynamic_main_orchestrator(
    req: func.HttpRequest, client: DurableOrchestrationClient
) -> func.HttpResponse:

    raw_body = req.get_json()

    try:
        data = RequestBodyChecker(
            **raw_body
        )
    except ValidationError as err:
        return func.HttpResponse(
            json.dumps({"detail": err.errors()}, ensure_ascii=False),
            status_code=422,
            mimetype="application/json",
        )
    except Exception as exc:
        return func.HttpResponse(
            json.dumps({"detail": f"서버 오류: {exc}"}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )

    try:
        client_input = data.model_dump(mode="json", exclude_none=True)

        instance_id = await client.start_new(
            orchestration_function_name="meritz_orchestrator",
            client_input=client_input,
        )
        return client.create_check_status_response(req, instance_id)
    except Exception as exc:
        return func.HttpResponse(
            json.dumps({"detail": f"서버 오류: {exc}"}, ensure_ascii=False),
            status_code=500,
            mimetype="application/json",
        )

