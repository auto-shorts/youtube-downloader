from pprint import pprint
from typing import Protocol

from youtube_transcript_api import TranscriptList, YouTubeTranscriptApi

from auto_shorts.download.models.transcription import (
    Language,
    OneLanguageTranscription,
    TranscriptionData,
    TranscriptionItem,
)
from auto_shorts.exceptions import DifferentBaseLanguagesException


class YoutubeTranscriptionInterface(Protocol):
    def get_transcription(self, video_id: str, base_language_code: str = 'en') -> TranscriptionData:
        ...

    async def get_transcription_async(
        self, video_id: str
    ) -> TranscriptionData:
        ...


class YoutubeTranscription:
    def __init__(self):
        self.transcript_api = YouTubeTranscriptApi()

    def _list_transcripts(self, video_id: str) -> TranscriptList:
        return self.transcript_api.list_transcripts(video_id)

    def get_transcription(self, video_id: str) -> TranscriptionData:
        """
        Thing to consider later - add possibility to translate.
        For now, it would take much time and is not needed
        """
        transcript_list = self._list_transcripts(video_id)

        transcription_results = {}
        for transcript in transcript_list:
            transcript_parts = transcript.fetch()
            transcription_items = [
                TranscriptionItem(
                    time_start_s=transcript_part["start"],
                    time_end_s=transcript_part["duration"]
                    + transcript_part["start"],
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

    async def get_transcription_async(
        self, video_id: str
    ) -> TranscriptionData:
        return self.get_transcription(video_id=video_id)


if __name__ == "__main__":
    video_id_test = "t7-nb1wlnyA"
    trans = YoutubeTranscription()
    pprint(trans.get_transcription(video_id_test))
