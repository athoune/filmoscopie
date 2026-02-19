# Models
# ======
# * https://huggingface.co/Qdrant/clip-ViT-B-32-vision
# * https://huggingface.co/Qdrant/resnet50-onnx (Micrsoft)
#
# See
# ---
# * https://codecalamity.com/cartoon-or-photo-image-detection-with-python/


from itertools import batched
from pathlib import Path
from typing import Generator

import numpy as np
from fastembed import ImageEmbedding
from PIL import Image


class Embedator:
    """
    Load model once, embed many time.
    """

    def __init__(self):
        self.model = ImageEmbedding(model_name="Qdrant/clip-ViT-B-32-vision")
        print(self.model.embedding_size)

    def embed(
        self,
        images: Generator[tuple[Path, Image.Image], None, None],
    ) -> Generator[tuple[Path, np.ndarray], None, None]:
        """Embed of collection of images.

        Args:
            images (Generator[Image.Image, None, None])

        Yields:
            tuple[str, np.ndarray]: name, embeded image
        """
        i = 0
        for batch in batched(images, 10):
            n = 0
            print(batch[0])
            for embedding in self.model.embed(b[1] for b in batch):
                p: Path = batch[n][0]
                p = p.parent / f"{p.name}.ndarray"
                embedding.tofile(p)
                yield p, embedding
                n += 1
            i += len(batch)
        print("size:", i)


def _embed_is_cached(
    images: Generator[tuple[Path, Image.Image], None, None],
) -> Generator[tuple[Path, Image.Image, np.ndarray | None], None, None]:
    for path, image in images:
        p = path.parent / f"{path.name}.ndarray"
        if p.exists():
            yield path, image, np.fromfile(p, np.float32)
        else:
            yield path, image, None


def classes(folder: Path) -> Generator[Path, None, None]:
    """Enumerate all classes in a folder."""
    return (c for c in folder.iterdir() if not c.name.startswith("."))


def _video_not_indexed(v: Generator[Path, None, None]) -> Generator[Path, None, None]:
    return (
        video for video in v if not (video.parent / f"{video.name}-0.ndarray").exists()
    )
