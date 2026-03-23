import httpx
from typing import Any, Dict, Optional


async def make_dynamic_request(
    url: str,
    method: str,
    body: Optional[Dict[str, Any]] = None,
    query_string: Optional[Dict[str, str]] = None,
    headers: Optional[Dict[str, str]] = None,
    data: Optional[Any] = None,
    files: Optional[Dict[str, Any]] = None,
) -> httpx.Response:
    """
    Execute an asynchronous HTTP request composing it dynamically with the provided parameters.
    """
    # initialize the asynchronous http client instance
    async with httpx.AsyncClient() as client:
        # pass all parameters to the generic request method
        response = await client.request(
            method=method,
            url=url,
            json=body,
            params=query_string,
            headers=headers,
            data=data,
            files=files,
        )
        # return the standard response object
        return response
