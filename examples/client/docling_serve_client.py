import logging
import time

import httpx
import json
from pathlib import Path
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from docling_serve.datamodel.responses import (
    HealthCheckResponse,
    TaskStatusResponse
)

logger = logging.getLogger(__name__)

WAITING_TIME = 2

API_URL = "http://localhost:5001"
PROCESS_URL_ENDPOINT = "/v1/convert/source/async"
PROCESS_FILE_ENDPOINT = "/v1/convert/file/async"


class DoclingServeClient:
    def __init__(
            self,
            base_url: str = API_URL,
            api_key: Optional[str] = None,
            timeout: float = 120.0
    ):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.client = httpx.Client(timeout=timeout, limits=httpx.Limits(max_keepalive_connections=5))

    def close(self):
        self.client.close()

    def health(self) -> HealthCheckResponse:
        """GET /health"""
        resp = self._request("GET", "/health")
        return HealthCheckResponse.model_validate(resp.json())





    def convert_source_sync(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """POST /v1/convert/source (synchronous)"""
        resp = self._request("POST", "/v1/convert/source", json=request)
        return resp.json()

    def wait_for_success(self, task_id: str):
        status_is_done = False
        while status_is_done != True:
            time.sleep(WAITING_TIME)
            logger.debug("Task %s not done yet, sleeping %s seconds", task_id, WAITING_TIME)
            resp_task_status_obj = self._request("GET", f"/v1/status/poll/{task_id}", timeout=WAITING_TIME).json()
            if resp_task_status_obj["task_status"] == "success":
                status_is_done = True
        # self._request("GET", f"/v1/result/{task_obj_id}", timeout=WAITING_TIME)
        return status_is_done

    def process_file_async(self, files: List[Path | str], options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """POST /v1/convert/file (multipart file upload)"""
        files_data = []
        file_handles = []  # Track handles to close them later

        for file_path in files:
            file_path = Path(file_path)
            if not file_path.exists():
                raise FileNotFoundError(f"File not found: {file_path}")

            file_handle = open(file_path, "rb")
            file_handles.append(file_handle)
            files_data.append(("files", (file_path.name, file_handle, "application/pdf")))

        # **FIX**: Serialize options dict to JSON string for multipart form
        data: Dict[str, str] = {}
        #                         ("Docling (JSON)", "json"),
        #                         ("Markdown", "md"),
        #                         ("HTML", "html"),
        #                         ("Plain Text", "text"),
        #                         ("Doc Tags", "doctags")
        #

        if options:
            data["options"] = json.dumps(options)

        #Call to the endpoint "/v1/convert/file"
        try:
            resp = self._request("POST", PROCESS_FILE_ENDPOINT, files=files_data, data=data)
            task_obj = resp.json()
            task_obj_id = task_obj["task_id"]
            if self.wait_for_success(task_obj_id):
                result_obj = self.get_parsed_result(task_obj_id)
            #result_obj = self._request("GET", f"/v1/result/{task_obj_id}", timeout=WAITING_TIME)
            return result_obj.json()
        finally:
                # Always close file handles
            for file_handle in file_handles:
                file_handle.close()

    def task_status(self, task_id: str, wait: float = 0.0) -> TaskStatusResponse:
        """GET /v1/status/poll/{task_id}"""
        params = {"wait": wait}
        resp = self._request("GET", f"/v1/status/poll/{task_id}", params=params)
        return TaskStatusResponse.model_validate(resp.json())

    def get_parsed_result(self, task_id: str) -> Dict[str, Any]:
        """GET /v1/result/{task_id}"""
        resp = self._request("GET", f"/v1/result/{task_id}")
        return resp

    def clear_converters(self):
        """GET /v1/clear/converters"""
        resp = self._request("GET", "/v1/clear/converters")
        return resp.json()

    def _request(self, method: str, path: str, **kwargs) -> httpx.Response:
        url = f"{self.base_url}{path}"

        if self.api_key:
            kwargs["headers"] = kwargs.get("headers", {})
            kwargs["headers"]["Authorization"] = f"Bearer {self.api_key}"

        resp = self.client.request(method, url, **kwargs)

        if resp.status_code >= 400:
            try:
                error = resp.json()
            except:
                error = {"message": resp.text}
            raise Exception(f"API error {resp.status_code}: {error}")

        return resp

def process_url_async(client: DoclingServeClient,
        url_path, selected_option) -> TaskStatusResponse:
    """POST /v1/convert/source/async"""
    request = {
        "url": url_path,
        "model": "docling-minilm-legacy",  # arXiv PDFs often need this
        "pages": "all"
    }
    resp = client.request(method, url_path, **kwargs) # self._request("POST", PROCESS_URL_ENDPOINT, json=request)
    return TaskStatusResponse.model_validate(resp.json())

def process_pdf(
        client: DoclingServeClient,
        pdf_path, selected_option
):
    """Convert PDF to Markdown with proper async polling"""
    pdf_file = Path(pdf_path)
    if not pdf_file.exists():
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    #markdown
    options = {"output_format": selected_option}

    try:
        resp = client.process_file_async([pdf_path], options)

        # Extract markdown (multiple possible structures)
        def extract_markdown(data: Any) -> Optional[str]:
            if isinstance(data, dict):
                # Document to be processed, could be markdown, Json, html, text
                if "md_content" in data["document"]:
                    return str(data.get("document").get("md_content"))
            return None



        markdown = extract_markdown(resp)
        return markdown

    except Exception as e:
        raise RuntimeError(f"PDF conversion failed: {e}")




if __name__ == "__main__":

    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    client = DoclingServeClient("http://localhost:5001")

    try:
        # Health check
        print("Health:", client.health().status)

        # Convert PDF file
        markdown_file = process_pdf(client, "./pdf_storage/pasticceria_scandinava.pdf", "markdown")
        if markdown_file:
            print("✅ Markdown from file extracted successfully!")
            print(markdown_file[:100])
        else:
            print("❌ No markdown found in response")
        #
        markdown_url = process_url_async(client, "https://arxiv.org/pdf/2501.17887" )

        if markdown_url:
            print("✅ Markdown from file extracted successfully!")
            print(markdown_url[:100])
        else:
            print("❌ No markdown found in response")


    finally:
        client.close()