import json
from typing import Any
from youtube_search import YoutubeSearch
from yt_dlp import YoutubeDL


def fetch(terms: str):
    results: list[dict[str, Any]] = YoutubeSearch(terms, max_results=10).to_dict()
    print(results)
    url = results[0]["url_suffix"]
    params = {
        "paths": dict(home="./trailers"),
        "overwrites": False,
        # "write_all_thumbnails": True,
        "writesubtitles": True,
        "subtitleslangs": ["fr-.*", "en-.*"],
        "format": "worst",
    }
    url = f"https://youtube.com{url}"
    with YoutubeDL(params) as ydl:
        ydl.download(url)
        info = ydl.extract_info(url, download=False)

        # ℹ️ ydl.sanitize_info makes the info json-serializable
        print(json.dumps(ydl.sanitize_info(info), indent=2))


if __name__ == "__main__":
    import sys

    fetch(" ".join(sys.argv[1:]))
