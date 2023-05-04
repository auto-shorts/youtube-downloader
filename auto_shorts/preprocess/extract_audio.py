from typing import Callable

from moviepy.editor import VideoFileClip

AudioExtractor = Callable[[str, str], None]


def extract_audio_moviepy(video_path: str, audio_save_path: str) -> None:
    clip = VideoFileClip(video_path)
    clip.audio.write_audiofile(str(audio_save_path))
