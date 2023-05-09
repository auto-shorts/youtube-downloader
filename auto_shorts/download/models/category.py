from pydantic import BaseModel


class VideoCategory(BaseModel):
    region_code: str
    category_id: int
    category_title: str
    assignable: bool
