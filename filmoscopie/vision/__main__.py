from pathlib import Path
import sys

import tqdm

from .embed import Embedator, _video_not_indexed, classes
from .movie import scenes, videos

embedator = Embedator()
for class_ in classes(Path(sys.argv[1])):
    vv = list(_video_not_indexed(videos(class_)))
    total = len(vv)
    for video in tqdm.tqdm(vv, total=total, desc=class_.name):
        print(video)
        for name, embedded in embedator.embed(
            (frame_name, img) for frame_name, start, stop, img in scenes(video)
        ):
            print(name)
