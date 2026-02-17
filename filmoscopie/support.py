# Models
# ======
# * https://huggingface.co/Qdrant/clip-ViT-B-32-vision
# * https://huggingface.co/Qdrant/resnet50-onnx (Micrsoft)
#
# See
# ---
# * https://codecalamity.com/cartoon-or-photo-image-detection-with-python/
import fastembed
from scenedetect.frame_timecode import FrameTimecode


from fastembed import ImageEmbedding
from pathlib import Path
from typing import Generator
from itertools import batched
from PIL import Image
import numpy as np
from scenedetect import open_video, SceneManager, ContentDetector, VideoStream


def scenes(
    path: str,
) -> Generator[tuple[FrameTimecode, FrameTimecode, Image.Image], None, None]:
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
    video: VideoStream = open_video(str(path), backend="pyav")
    video.seek(5 * video.frame_rate)  # 5s
    scene_manager = SceneManager()
    scene_manager.add_detector(ContentDetector())

    scene_manager.detect_scenes(video=video)
    for start, end in scene_manager.get_scene_list():
        video.seek(start)
        img_array = video.read()
        assert img_array is not False
        yield start, end, Image.fromarray(img_array)


class Embedator:
    """
    Load model once, embed many time.
    """

    def __init__(self):
        self.model = ImageEmbedding(model_name="Qdrant/clip-ViT-B-32-vision")
        print(self.model.embedding_size)

    def embed(
        self,
        images: Generator[Image.Image, None, None],
    ) -> Generator[np.ndarray, None, None]:
        """Embed of collection of images.

        Args:
            images (Generator[Image.Image, None, None])

        Yields:
            Generator[np.ndarray, None, None]: embeded images
        """
        i = 0
        for batch in batched(images, 10):
            for embedding in self.model.embed(batch):
                yield embedding
            i += len(batch)
        print("size:", i)


def classes(folder: str | Path) -> Generator[Path, None, None]:
    """Enumerate all classes in a folder."""
    for class_ in Path(folder).iterdir():
        if class_.name.startswith("."):
            continue
        yield class_


def images(folder: str | Path) -> Generator[Path, None, None]:
    """_summary_

    Args:
        folder (str | Path): images folder

    Yields:
        Generator[Path, None, None]: image path
    """
    p = Path(folder)
    for img in p.glob("*.webp"):
        yield img
    for img in p.glob("*.jpg"):
        yield img


def videos(folder: str | Path) -> Generator[Path, None, None]:
    """All videos in a folder

    Args:
        folder (str | Path): video folder

    Yields:
        Generator[Path, None, None]: video path
    """
    p = Path(folder)
    for f in ("mkv", "mp4", "webm"):
        for video in p.glob(f"*.{f}"):
            yield video


if __name__ == "__main__":
    import sys

    embedator = Embedator()
    for class_ in classes(sys.argv[1]):
        print(class_.name)
        for video in videos(class_):
            print(video)
            for embedded in embedator.embed(i[2] for i in scenes(video)):
                pass
