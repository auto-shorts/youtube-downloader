from typing import Any, Callable

from pydantic import BaseModel


class FunctionInput(BaseModel):
    function: Callable
    args: list[Any] = []
    kwargs: dict[str, Any] = {}


class DownloadMetadata(BaseModel):
    s3_paths: list[str]
    download_date: str
    download_query: str
