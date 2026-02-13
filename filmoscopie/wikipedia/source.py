import bz2
import io
from pathlib import Path
from typing import Generator

import zstandard as zstd


def zstd_line_reader(source: str) -> Generator[str, None, None]:
    with open(source, "rb") as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            text_reader = io.TextIOWrapper(reader, encoding="utf-8")
            for line in text_reader:
                yield line


def source(dump_file="frwiki-latest-pages-articles.xml"):
    if Path(f"{dump_file}.zstd").exists():
        source = zstd_line_reader(f"{dump_file}.zstd")
    elif Path(f"{dump_file}.bz2").exists():
        source = bz2.open(f"{dump_file}.bz2", "rt")
    else:
        raise Exception(
            f"Dump file not found: {dump_file}  https://dumps.wikimedia.org/frwiki/latest/"
        )
    return source
