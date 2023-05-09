import pandas as pd
from pydantic import BaseModel


class TranscriptionItem(BaseModel):
    time_start_s: float
    time_end_s: float
    period_duration_s: float
    text: str


class Language(BaseModel):
    language_code: str
    language: str


class OneLanguageTranscription(Language):
    transcription: list[TranscriptionItem]
    is_generated: bool
    is_translatable: bool
    translation_languages: list[Language]

    def transcription_to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(
            [
                transcription_item.dict()
                for transcription_item in self.transcription
            ]
        ).sort_values(by="time_start_ms", ascending=True)


class TranscriptionData(BaseModel):
    transcriptions: dict[str, OneLanguageTranscription]
