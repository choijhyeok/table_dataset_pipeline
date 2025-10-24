from azure.durable_functions import DurableOrchestrationContext
from application.app import app
from pydantic import BaseModel
import logging 


from activities.crawler_list import meritz_crawler, get_cnt
from activities.table_parser import pdf_to_png_md


class RequestBodyChecker(BaseModel):
    url: str
    container: str
    company_name: str


@app.function_name(name="meritz_orchestrator")
@app.orchestration_trigger(context_name="context")
def meritz_orchestrator(context: DurableOrchestrationContext):
    try:
        file_info = context.get_input()
        
        # url = file_info.get("url")

        
        # crawler_cnt = yield context.call_activity("get_cnt", file_info)
        # crawler_tasks = []
        # for cnt in range(crawler_cnt.get("count")):
        #     task_input = {
        #         "url": url,
        #         "container": file_info.get("container"),
        #         "cnt": cnt
        #     }
        #     crawler_tasks.append(
        #         context.call_activity("meritz_crawler", task_input)
        #     )
        
        # if crawler_tasks:
        #     yield context.task_all(crawler_tasks)
            
            
            ## parsing
            
        logging.error("Parsing started...")
        result = yield context.call_activity("pdf_to_png_md", file_info)
        logging.error("Parsing end...")

        return {
            "success": True,
            "status": "succeeded"
        }
        

    except Exception as e:
        raise