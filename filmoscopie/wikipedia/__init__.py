import json
import sqlite3
import time
from hashlib import sha256
from itertools import batched
from typing import Any, Generator, TextIO

from wiki_dump_reader import Cleaner, iterate

from .page import extract_film_data, is_draft, is_film_article, is_sub_theme

DB_NAME = "wikipedia.db"


def hashes(*args) -> tuple[str, ...]:
    r: list[str] = [""] * len(args)
    for i, data in enumerate(args):
        m = sha256()
        m.update(data.encode())
        r[i] = m.hexdigest()
    return tuple(r)


class WikipediaFilmExtractor:
    """
    Extracts film data from French Wikipedia dump and writes to JSON Lines format.

    Extracts:
    - Basic info (title, director, year, etc.)

    - Synopsis (plot summary)
    - English title
    - IMDb ID
    """

    def __init__(self, dump_source: TextIO):
        self.dump_source = dump_source
        self.films_count = 0
        self.pages_processed = 0
        self.draft_count = 0
        self.draft_writer = None
        self._init_db()

        # Initialize wiki cleaner to remove markup
        self.cleaner = Cleaner()

    def _init_db(self):
        self.con: sqlite3.Connection = sqlite3.connect(DB_NAME)
        cur: sqlite3.Cursor = self.con.cursor()
        for sql in [
            """CREATE TABLE IF NOT EXISTS
                movie(id INTEGER PRIMARY KEY,
                title TEXT NOT NULL,
                title_hash TEXT NOT NULL,
                text_hash TEXT NOT NULL,
                data JSONB,
                mtime FLOAT NOT NULL);
            """,
            "CREATE INDEX IF NOT EXISTS movie_title_hash ON movie(title_hash)",
        ]:
            cur.execute(sql)
        max_id: int | None = cur.execute("SELECT max(id) from movie").fetchone()[0]
        self.max_id: int = 0 if max_id is None else max_id

    def parse_dump(self):
        """
        Parse the Wikipedia dump incrementally and write films to JSON Lines.
        Memory efficient: processes one page at a time.
        """
        print(f"Opening dump: {self.dump_source}")
        print("Starting incremental parsing...\n")

        self.draft_writer = open("films_without_draft.txt", "w")
        cursor = self.con.cursor()
        current_id = self.max_id
        chrono = time.time_ns()
        mtime = time.time()
        for batch in batched(self._pages(), 50):
            title_hashes = [hashes(t)[0] for t, _ in batch]
            cursor.execute(
                # FIXME no template in SQL
                "SELECT title_hash, text_hash FROM movie WHERE title_hash IN (%s);"
                % ",".join(f"'{t}'" for t in title_hashes),
            )
            r = cursor.fetchall()
            if r is None:
                olds = dict[str, str]()
            else:
                olds = dict[str, str](r)
            for title, text in batch:
                title_hash, text_hash = hashes(title, text)
                old_text_hash = olds.get(title_hash)
                if old_text_hash == text_hash:
                    cursor.execute(
                        "UPDATE movie SET mtime=:mtime WHERE title_hash=:id",
                        dict(mtime=mtime, id=title_hash),
                    )
                else:
                    film = extract_film_data(title, text)
                    if old_text_hash is None:  # New movie
                        current_id += 1
                        id_ = current_id
                        cursor.execute(
                            """INSERT INTO
                                    movie(id, title, title_hash, text_hash, data, mtime)
                                VALUES(:id, :title, :title_hash, :text_hash, :data, :mtime);""",
                            dict(
                                id=id_,
                                title=title,
                                title_hash=title_hash,
                                text_hash=text_hash,
                                data=json.dumps(film),
                                mtime=mtime,
                            ),
                        )
                    else:
                        # Modified movie
                        cursor.execute(
                            """UPDATE movie
                                SET text_hash=':text_hash'
                                SET data = jsonb(:data)
                                SET mtime = :mtime
                                WHERE title_hash=:id;""",
                            dict(
                                id=title_hash,
                                text_hash=text_hash,
                                data=json.dumps(film),
                                mtime=mtime,
                            ),
                        )

                self.films_count += 1

                # Display progress every 100 films
                if self.films_count % 100 == 0:
                    now = time.time_ns()
                    print(
                        f"✓ {self.films_count} films extracted "
                        f"({self.pages_processed:,} pages processed) in "
                        f"{(now - chrono) / 10**9:.2f}s"
                    )
                    chrono = now
            self.con.commit()

        self.con.commit()
        print(f"\n{'=' * 60}")
        print("Extraction complete!")
        print(f"  - Pages processed: {self.pages_processed:,}")
        print(f"  - Films extracted: {self.films_count:,}")
        print(f"{'=' * 60}")

    def _pages(self) -> Generator[tuple[str, str], None, None]:
        # Iterate through pages in the dump
        for title, text in iterate(self.dump_source):
            self.pages_processed += 1
            if is_film_article(text) and not is_draft(text) and not is_sub_theme(text):
                yield title, text


def movies_documents() -> tuple[
    Generator[tuple[int, str, dict[str, Any]], None, None], int
]:
    connection = sqlite3.connect(DB_NAME)
    cursor = connection.cursor()
    cursor.execute("SELECT count(*) FROM movie")
    (total,) = cursor.fetchone()

    def _loop():
        i = 0
        cursor.execute("SELECT id, title, json(data) FROM movie")
        for id_, title, movie in cursor:
            movie = json.loads(movie)
            text: str = movie["synopsis"]
            payload = dict(
                title=f"{title} {movie['year']}",
                genre=movie["genre"],
                duration=movie["duration_minutes"],
                year=movie["year"],
                imdb=movie["imdb_id"],
            )
            yield i, text, payload
            i += 1

    return (
        _loop(),
        total,
    )
