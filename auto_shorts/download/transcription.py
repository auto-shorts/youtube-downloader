from pprint import pprint
from typing import Protocol

import pandas as pd
from pydantic import BaseModel
from youtube_transcript_api import TranscriptList, YouTubeTranscriptApi

from auto_shorts.exceptions import DifferentBaseLanguagesException


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
            [transcription_item.dict() for transcription_item in self.transcription]
        ).sort_values(by="time_start_ms", ascending=True)


class TranscriptionData(BaseModel):
    transcriptions: dict[str, OneLanguageTranscription]


class YoutubeTranscriptionInterface(Protocol):
    def get_transcription(self, video_id: str) -> TranscriptionData:
        ...


class YoutubeTranscription:
    def __init__(self):
        self.transcript_api = YouTubeTranscriptApi()

    def _list_transcripts(self, video_id: str) -> TranscriptList:
        return self.transcript_api.list_transcripts(video_id)

    @staticmethod
    def _validate_transcripts(transcript_list: TranscriptList):
        main_language_codes = []
        for transcript in transcript_list:
            main_language_codes.append(transcript.language_code)

            if not all(x == main_language_codes[0] for x in main_language_codes):
                """
                This will allow to check if there is a situation, when
                there are 2 main languages. I haven't managed to reproduce it,
                but maybe it is possible.
                """
                raise DifferentBaseLanguagesException(
                    language_codes=main_language_codes
                )

    def get_transcription(self, video_id: str) -> TranscriptionData:
        """
        Thing to consider later - add possibility to translate.
        For now, it would take much time and is not needed
        """
        transcript_list = trans._list_transcripts(video_id)
        self._validate_transcripts(transcript_list)

        transcription_results = {}
        for transcript in transcript_list:
            transcript_parts = transcript.fetch()
            transcription_items = [
                TranscriptionItem(
                    time_start_s=transcript_part["start"],
                    time_end_s=transcript_part["duration"] + transcript_part["start"],
                    period_duration_s=transcript_part["duration"],
                    text=transcript_part["text"],
                )
                for transcript_part in transcript_parts
            ]
            one_language_transcription = OneLanguageTranscription(
                transcription=transcription_items,
                language=transcript.language,
                language_code=transcript.language_code,
                is_generated=transcript.is_generated,
                is_translatable=transcript.is_translatable,
                translation_languages=[
                    Language(
                        language=lang_info["language"],
                        language_code=lang_info["language_code"],
                    )
                    for lang_info in transcript.translation_languages
                ],
            )
            language_key = (
                one_language_transcription.language_code
                if not one_language_transcription.is_generated
                else f"{one_language_transcription.language_code}_automatic"
            )
            transcription_results[language_key] = one_language_transcription

        return TranscriptionData(transcriptions=transcription_results)

if __name__ == "__main__":
    video_id_test = "t7-nb1wlnyA"
    trans = YoutubeTranscription()
    pprint(trans.get_transcription(video_id_test).dict())
