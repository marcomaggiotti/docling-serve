import logging
import time

import httpx
import json
from pathlib import Path
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
logger = logging.getLogger(__name__)

WAITING_TIME = 2

class HealthCheckResponse(BaseModel):
    status: str

class TaskStatusResponse(BaseModel):
    task_id: str
    task_type: str
    task_status: str
    task_position: Optional[int] = None
    task_meta: Optional[Dict[str, Any]] = None


class DoclingServeClient:
    def __init__(
            self,
            base_url: str = "http://localhost:5001",
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

    def convert_source_async(self, request: Dict[str, Any]) -> TaskStatusResponse:
        """POST /v1/convert/source/async"""
        resp = self._request("POST", "/v1/convert/source/async", json=request)
        return TaskStatusResponse.model_validate(resp.json())

    def convert_file_sync(self, files: List[Path | str], options: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
        if options:
            data["options"] = json.dumps(options)

        #Call to the endpoint "/v1/convert/file"
        try:
            resp = self._request("POST", "/v1/convert/file/async", files=files_data, data=data)
            task_obj = resp.json()
            task_obj_id = task_obj["task_id"]
            status_is_done = False
            while status_is_done != True:
                time.sleep(WAITING_TIME)
                logger.debug("Task %s not done yet, sleeping %s seconds", task_obj_id, WAITING_TIME)
                resp_task_status_obj = self._request("GET", f"/v1/status/poll/{task_obj_id}", timeout=WAITING_TIME).json()
                if resp_task_status_obj["task_status"] == "success":
                    status_is_done = True
            #self._request("GET", f"/v1/result/{task_obj_id}", timeout=WAITING_TIME)
            result_obj = self._request("GET", f"/v1/result/{task_obj_id}", timeout=WAITING_TIME)
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

    def task_result(self, task_id: str) -> Dict[str, Any]:
        """GET /v1/result/{task_id}"""
        resp = self._request("GET", f"/v1/result/{task_id}")
        return resp.json()

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


def convert_pdf_to_markdown(
        client: DoclingServeClient,
        pdf_path
) -> Optional[str]:
    """Convert PDF to Markdown with proper async polling"""
    pdf_file = Path(pdf_path)
    if not pdf_file.exists():
        raise FileNotFoundError(f"PDF file not found: {pdf_path}")

    options = {"output_format": "markdown"}

    try:
        resp = client.convert_file_sync([pdf_path], options)

        # Extract markdown (multiple possible structures)
        def extract_markdown(data: Any) -> Optional[str]:
            if isinstance(data, dict):
                # D
                if "md_content" in data["document"]:

                    return str(data.get("document").get("md_content"))

            return None

        markdown = extract_markdown(resp)
        return markdown

    except Exception as e:
        raise RuntimeError(f"PDF conversion failed: {e}")


# Usage example
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
        markdown = convert_pdf_to_markdown(client, "./pdf_storage/pasticceria_scandinava.pdf")
        if markdown:
            print("✅ Markdown extracted successfully!")
            print(markdown[:1000])
        else:
            print("❌ No markdown found in response")

    finally:
        client.close()