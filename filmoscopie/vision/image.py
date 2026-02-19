from pathlib import Path
from typing import Generator


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
