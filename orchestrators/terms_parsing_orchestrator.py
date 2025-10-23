from azure.durable_functions import DurableOrchestrationContext
from application.app import app
from activities.crawler_list import meritz_crawler, get_cnt
from pydantic import BaseModel
import logging 


class RequestBodyChecker(BaseModel):
    url: str
    container: str
    company_name: str

import azure.durable_functions as df
from application.app import app



@app.function_name(name="meritz_orchestrator")
@app.orchestration_trigger(context_name="context")
def meritz_orchestrator(context: DurableOrchestrationContext):
    try:
        file_info = context.get_input()
        
        url = file_info.get("url")
        company_name = file_info.get("company_name") 
        
        
        crawler_cnt = yield context.call_activity("get_cnt", file_info)
        
        
         
        crawler_tasks = []
        for cnt in range(crawler_cnt.get("count")):
            task_input = {
                "url": url,
                "container": file_info.get("container"),
                "cnt": cnt
            }
            crawler_tasks.append(
                context.call_activity("meritz_crawler", task_input)
            )
        
        if crawler_tasks:
            yield context.task_all(crawler_tasks)
        
        
        # file_check = yield context.call_activity("meritz_crawler", file_info)
        
        return {
            "success": True,
            "status": "succeeded"
        }
        
        # if file_check.get("success") is False:
        #     log_orch_event(
        #         message=f"crawler 실패 : {file_check.get('error')}",
        #         company=company_name,
        #         status="Failed",
        #         orchestrator="terms_parsing_orchestrator",
        #         activity="meritz_crawler",
        #         url=url,
        #     )
        #     return {
        #         "success": False,
        #         "status": "failed",
        #         "error": file_info.get("error"),
        #     }
        # else:
        #     log_orch_event(
        #         message=f"crawler 완료",
        #         instance_id=instance_id,
        #         status="Completed",
        #         orchestrator="process_eterms_orchestrator",
        #         activity="probe_pdf_pages",
        #         filename=filename,
        #     )

        # total_pages = file_check.get("pages")

        # if not isinstance(total_pages, int) or total_pages <= 0:
        #     log_orch_event(
        #         message="PDF 페이지 수가 유효하지 않습니다.",
        #         instance_id=instance_id,
        #         status="Failed",
        #         orchestrator="process_eterms_orchestrator",
        #         activity="probe_pdf_pages",
        #         filename=filename,
        #     )
        #     return {
        #         "success": False,
        #         "status": "failed",
        #         "error": "invalid_page_count",
        #     }

        # # 소규모 문서는 한번에 처리
        # if total_pages <= 50:
        #     save_result = yield context.call_activity("extract_pdf_full", file_info)

        #     if save_result.get("success") is False:
        #         log_orch_event(
        #             message=f"콘텐츠 추출 실패: {save_result.get('error')}",
        #             instance_id=instance_id,
        #             status="Failed",
        #             orchestrator="process_eterms_orchestrator",
        #             activity="extract_pdf_full",
        #             filename=filename,
        #         )
        #         return {
        #             "success": False,
        #             "status": "failed",
        #             "error": save_result.get("error"),
        #         }
        #     else:
        #         log_orch_event(
        #             message=f"콘텐츠 추출 완료",
        #             instance_id=instance_id,
        #             status="Completed",
        #             orchestrator="process_eterms_orchestrator",
        #             activity="extract_pdf_full",
        #             filename=filename,
        #         )

        #     return {
        #         "success": True,
        #         "status": "succeeded",
        #         "file": file_info,
        #         "result": {"csv_blob_path": save_result.get("csv_blob_path")},
        #     }

        # # 대규모 문서는 배치로 처리
        # else:
        #     batch_size = 50
        #     batch_tasks = []

        #     for start in range(0, total_pages, batch_size):
        #         end = min(start + batch_size - 1, total_pages - 1)
        #         task_input = {
        #             "file_path": file_info.get("file_path"),
        #             "category_code": file_info.get("category_code"),
        #             "file_name": file_info.get("file_name"),
        #             "reg_date": file_info.get("reg_date"),
        #             "chg_date": file_info.get("chg_date"),
        #             "sub_category_name" : file_info.get("sub_category_name"),
        #             "document_id" : file_info.get("document_id"),
        #             "terms_type" : file_info.get("terms_type"),
        #             "company_name" : file_info.get("company_name"),
        #             "start": start,
        #             "end": end,
        #         }
        #         batch_tasks.append(
        #             context.call_activity("process_page_batch", task_input)
        #         )

        #     batch_results = []
        #     if batch_tasks:
        #         batch_results = yield context.task_all(batch_tasks)

        #     aggregated_paths = []
        #     for idx, batch_result in enumerate(batch_results):
        #         if not batch_result or batch_result.get("success") is False:
        #             error_msg = (
        #                 batch_result.get("error")
        #                 if isinstance(batch_result, dict)
        #                 else "unknown"
        #             )
        #             log_orch_event(
        #                 message=f"콘텐츠 배치 추출 실패 (batch {idx}): {error_msg}",
        #                 instance_id=instance_id,
        #                 status="Failed",
        #                 orchestrator="process_eterms_orchestrator",
        #                 activity="process_page_batch",
        #                 filename=filename,
        #             )
        #             return {
        #                 "success": False,
        #                 "status": "failed",
        #                 "error": error_msg,
        #             }

        #         aggregated_paths.extend(batch_result.get("paths", []))

        #     if not aggregated_paths:
        #         log_orch_event(
        #             message="콘텐츠 배치 추출 결과가 비어 있습니다.",
        #             instance_id=instance_id,
        #             status="Failed",
        #             orchestrator="process_eterms_orchestrator",
        #             activity="process_page_batch",
        #             filename=filename,
        #         )
        #         return {
        #             "success": False,
        #             "status": "failed",
        #             "error": "empty_batch_paths",
        #         }

        #     log_orch_event(
        #         message="콘텐츠 배치 추출 완료",
        #         instance_id=instance_id,
        #         status="Completed",
        #         orchestrator="process_eterms_orchestrator",
        #         activity="process_page_batch",
        #         filename=filename,
        #     )

        #     merge_input = {
        #         "paths": aggregated_paths,
        #         "category_code": file_info.get("category_code"),
        #         "file_name": file_info.get("file_name"),
        #         "reg_date": file_info.get("reg_date"),
        #         "chg_date": file_info.get("chg_date"),
        #         "sub_category_name" : file_info.get("sub_category_name"),
        #         "document_id" : file_info.get("document_id"),
        #         "terms_type" : file_info.get("terms_type"),
        #         "company_name" : file_info.get("company_name")
        #     }

        #     merge_result = yield context.call_activity("merge_results", merge_input)

        #     if merge_result.get("success") is False:
        #         log_orch_event(
        #             message=f"콘텐츠 병합 실패: {merge_result.get('error')}",
        #             instance_id=instance_id,
        #             status="Failed",
        #             orchestrator="process_eterms_orchestrator",
        #             activity="merge_results",
        #             filename=filename,
        #         )
        #         return {
        #             "success": False,
        #             "status": "failed",
        #             "error": merge_result.get("error"),
        #         }

        #     log_orch_event(
        #         message="콘텐츠 병합 완료",
        #         instance_id=instance_id,
        #         status="Completed",
        #         orchestrator="process_eterms_orchestrator",
        #         activity="merge_results",
        #         filename=filename,
        #     )

            # return {
            #     "success": True,
            #     "status": "succeeded",
            #     "file": file_info,
            #     "result": {"csv_blob_path": merge_result.get("csv_blob_path")},
            # }

    except Exception as e:
        # log_orch_event(
        #     message="Orchestration failed",
        #     instance_id=instance_id,
        #     status="Failed",
        #     orchestrator="process_eterms_orchestrator",
        #     error=str(e),
        #     filename=filename,
        # )
        raise