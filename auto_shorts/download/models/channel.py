from pydantic import BaseModel

class ChannelInfo(BaseModel):
    channel_id: str
    title: str
    description: str
    custom_url: str | None
    views: int
    subscribers: int