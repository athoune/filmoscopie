from pathlib import Path
from typing import Generator

from PIL import Image
from scenedetect import ContentDetector, SceneManager, VideoStream, open_video
from scenedetect.frame_timecode import FrameTimecode


def scenes(
    path: Path,
) -> Generator[tuple[Path, FrameTimecode, FrameTimecode, Image.Image], None, None]:
    """
    Extract scenes from a video.

    Args:
        path (str): video file path.

    Yields
        Generator[tuple[FrameTimecode, FrameTimecode, Image.Image], None, None]:
            * start time
            * end time
            * image of the first frame
    """
    name = path.name.replace("/", "--")
    home = path.parent
    video: VideoStream = open_video(str(path), backend="pyav")
    try:
        video.seek(5 * video.frame_rate)  # 5s
    except Exception as e:
        print("av Exception with", str(path), ":", e)
        return
    scene_manager = SceneManager()
    scene_manager.add_detector(ContentDetector())

    scene_manager.detect_scenes(video=video)
    for i, (start, end) in enumerate(scene_manager.get_scene_list()):
        video.seek(start)
        img_array = video.read()
        assert img_array is not False
        yield home / f"{name}-{i}", start, end, Image.fromarray(img_array)


def videos(folder: Path) -> Generator[Path, None, None]:
    """All videos in a folder

    Args:
        folder (str | Path): video folder

    Yields:
        Generator[Path, None, None]: video path
    """
    for format_ in ("mkv", "mp4", "webm"):
        for video in folder.glob(f"*.{format_}"):
            yield video
