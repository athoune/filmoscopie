import bz2
import json
import re
import sqlite3
from io import StringIO
from pathlib import Path
from hashlib import sha256
from typing import Dict, Optional, TextIO, Any, Generator
from itertools import batched
import time
import io

import zstandard as zstd
from wiki_dump_reader import Cleaner, iterate

DB_NAME = "wikipedia.db"


def titles(txt: str):
    "Find all titles in a page using the Wikipedia markup."
    title = re.compile(r"^(=+) (.*?) (=+)$")
    poz = 0
    for i, line in enumerate(StringIO(txt).readlines()):
        m = title.match(line)
        if m is not None:
            assert m[1] == m[3]
            yield i, poz, m[2]
        poz += len(line)


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

    def __init__(self, dump_source: TextIO, output_path: str):
        self.dump_source = dump_source
        self.output_path = output_path
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
        print(f"Output file: {self.output_path}")
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
                    film = self._extract_film_data(title, text)
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
        print(f"  - Output file: {self.output_path}")
        print(f"{'=' * 60}")

    def _pages(self) -> Generator[tuple[str, str], None, None]:
        # Iterate through pages in the dump
        for title, text in iterate(self.dump_source):
            self.pages_processed += 1
            if self._is_film_article(text) and not self._is_draft(text):
                yield title, text

    def _is_draft(self, text: str) -> bool:
        return text.find("{{ébauche|film") != -1

    def _is_film_article(self, text: str) -> bool:
        """
        Detect if the article is about a film by looking for film infoboxes.
        """
        infobox_patterns = [
            r"\{\{Infobox Film",
            r"\{\{Infobox film",
        ]

        cine = re.search(r"\{\{Infobox Cinéma.*", text, re.IGNORECASE)
        # * personnalité
        # * festival
        # * film
        if cine is not None and cine[0].find("(personnalité)") == -1:
            return True
        return any(
            re.search(pattern, text, re.IGNORECASE) for pattern in infobox_patterns
        )

    def _extract_film_data(self, title: str, text: str) -> dict[str, Any]:
        """
        Extract structured data from a film article.

        Extracts:
        - Basic metadata (director, year, country, etc.)
        - Synopsis/plot summary
        - English title
        - IMDb ID

        Args:
            title: Film title (page title)
            text: Article wikitext

        Returns:
            Dictionary with extracted film data
        """
        film_data = {
            "title": re.sub(r"\((télé)?film.*\)", "", title).strip(),
            "original_title": None,
            "english_title": None,  # NEW
            "director": None,
            "year": None,
            "country": None,
            "genre": None,
            "duration_minutes": None,
            "actors": [],
            "writer": None,
            "producer": None,
            "budget": None,
            "imdb_id": None,  # NEW
            "synopsis": None,  # NEW
        }
        # Find the infobox
        infobox_match = re.search(
            r"\{\{Infobox[^}]*?(Cinéma|Film|film)\s*\|?(.*?)\n\}\}",
            text,
            re.DOTALL | re.IGNORECASE,
        )

        if not infobox_match:
            return film_data

        infobox_content = infobox_match.group(2)

        # ===== EXTRACT BASIC FIELDS =====
        field_patterns = {
            "original_title": r"titre original\s*=\s*(.+)",
            "director": r"réalisation\s*=\s*(.+)",
            "writer": r"scénario\s*=\s*(.+)",
            "producer": r"(?:producteur|production)\s*=\s*(.+)",
            "country": r"pays\s*=\s*(.+)",
            "genre": r"genre\s*=\s*(.+)",
            "budget": r"budget\s*=\s*(.+)",
        }

        for field, pattern in field_patterns.items():
            match = re.search(pattern, infobox_content, re.IGNORECASE)
            if match:
                film_data[field] = self._clean_value(match.group(1))

        # === GENRE ===#
        if film_data["genre"] is None:
            film_data["genre"] = []
        else:
            film_data["genre"] = [
                a.strip().lower() for a in film_data["genre"].split((","))
            ]
        # ===== EXTRACT YEAR =====
        year_match = re.search(r"année\s*=\s*(\d{4})", infobox_content, re.IGNORECASE)
        if year_match:
            film_data["year"] = int(year_match.group(1))

        # Alternative: extract from release date
        if not film_data["year"]:
            date_match = re.search(
                r"(?:sortie|date)\s*=.*?(\d{4})", infobox_content, re.IGNORECASE
            )
            if date_match:
                film_data["year"] = int(date_match.group(1))

        # ===== EXTRACT DURATION =====
        duration_match = re.search(r"durée\s*=\s*(\d+)", infobox_content, re.IGNORECASE)
        if duration_match:
            film_data["duration_minutes"] = int(duration_match.group(1))

        # ===== EXTRACT ACTORS =====
        actors_match = re.search(
            r"acteur\s*=\s*(.+?)(?:\n\||\n\}\})",
            infobox_content,
            re.IGNORECASE | re.DOTALL,
        )
        if actors_match:
            film_data["actors"] = self._parse_list(actors_match.group(1))

        # ===== EXTRACT ENGLISH TITLE =====
        # Method 1: Look for "titre anglais" field in infobox
        english_title_match = re.search(
            r"titre anglais\s*=\s*(.+)", infobox_content, re.IGNORECASE
        )
        if english_title_match:
            film_data["english_title"] = self._clean_value(english_title_match.group(1))

        # Method 2: Look for interlanguage links (less reliable)
        if not film_data["english_title"]:
            # Try to find {{Titre en langue|en|English Title}}
            lang_title_match = re.search(
                r"\{\{Titre en langue\|en\|([^}]+)\}\}", text, re.IGNORECASE
            )
            if lang_title_match:
                film_data["english_title"] = self._clean_value(
                    lang_title_match.group(1)
                )

        # ===== EXTRACT IMDB ID =====
        # Method 1: Look for IMDb template {{IMDb titre|id=...}}
        imdb_template_match = re.search(
            r"\{\{IMDb\s+titre\s*\|\s*(?:id\s*=\s*)?([a-z]{2}\d+)", text, re.IGNORECASE
        )
        if imdb_template_match:
            film_data["imdb_id"] = imdb_template_match.group(1)

        # Method 2: Look for direct IMDb URL
        if not film_data["imdb_id"]:
            imdb_url_match = re.search(r"imdb\.com/title/(tt\d+)", text, re.IGNORECASE)
            if imdb_url_match:
                film_data["imdb_id"] = imdb_url_match.group(1)

        # Method 3: Look for "IMDb" or "IMDB" field in infobox or external links section
        if not film_data["imdb_id"]:
            imdb_field_match = re.search(
                r"(?:IMDb|IMDB)\s*=\s*([a-z]{2}\d+)", text, re.IGNORECASE
            )
            if imdb_field_match:
                film_data["imdb_id"] = imdb_field_match.group(1)

        # ===== EXTRACT SYNOPSIS =====
        film_data["synopsis"] = self._extract_synopsis(text)

        return film_data

    def _extract_synopsis(self, text: str) -> Optional[str]:
        """
        Extract the synopsis/plot summary from the article.

        French Wikipedia typically has sections like:
        - == Synopsis ==
        - == Résumé ==
        - == Histoire ==
        - == Intrigue ==

        Args:
            text: Full article wikitext

        Returns:
            Cleaned synopsis text or None
        """
        # Common section headers for synopsis in French
        synopsis_patterns = [
            r"==\s*Synopsis\s*==\s*\n(.*?)(?:\n==|\Z)",
            r"==\s*Résumé\s*==\s*\n(.*?)(?:\n==|\Z)",
            r"==\s*Histoire\s*==\s*\n(.*?)(?:\n==|\Z)",
            r"==\s*Intrigue\s*==\s*\n(.*?)(?:\n==|\Z)",
            r"==\s*Scénario\s*==\s*\n(.*?)(?:\n==|\Z)",
        ]

        for pattern in synopsis_patterns:
            match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)
            if match:
                synopsis = match.group(1)

                # Clean the synopsis
                synopsis = self._clean_synopsis(synopsis)

                # Only return if we have substantial content (at least 50 chars)
                if len(synopsis) >= 50:
                    return synopsis

        return None

    def _clean_synopsis(self, synopsis: str) -> str:
        """
        Clean synopsis text by removing wiki markup and formatting.

        Args:
            synopsis: Raw synopsis text with wiki markup

        Returns:
            Cleaned plain text synopsis
        """
        # Remove subsection headers (=== ... ===)
        synopsis = re.sub(r"={2,}.*?={2,}", "", synopsis)

        # Remove wiki links [[Link|Text]] -> Text
        synopsis = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", synopsis)

        # Remove external links [http://... Text] -> Text
        synopsis = re.sub(r"\[https?://[^\s\]]+\s+([^\]]+)\]", r"\1", synopsis)
        synopsis = re.sub(r"\[https?://[^\s\]]+\]", "", synopsis)

        # Remove HTML tags
        synopsis = re.sub(r"<[^>]+>", "", synopsis)

        # Remove references {{Référence...}} or <ref>...</ref>
        synopsis = re.sub(r"\{\{[Rr]éférence[^}]*\}\}", "", synopsis)
        synopsis = re.sub(r"<ref[^>]*>.*?</ref>", "", synopsis, flags=re.DOTALL)
        synopsis = re.sub(r"<ref[^>]*/?>", "", synopsis)

        # Remove templates {{...}}
        # This is tricky because templates can be nested
        # We'll do a simple removal for common cases
        synopsis = re.sub(r"\{\{[^}]+\}\}", "", synopsis)

        # Remove bold/italic formatting
        synopsis = re.sub(r"'{2,}", "", synopsis)

        # Remove multiple newlines and spaces
        synopsis = re.sub(r"\n+", "\n", synopsis)
        synopsis = re.sub(r" +", " ", synopsis)

        # Remove leading/trailing whitespace
        synopsis = synopsis.strip()

        # Limit length to avoid huge synopses (max 2000 chars)
        if len(synopsis) > 2000:
            # Try to cut at a sentence boundary
            cutoff = synopsis.rfind(".", 0, 2000)
            if cutoff > 1000:  # Only cut if we have substantial content
                synopsis = synopsis[: cutoff + 1]
            else:
                synopsis = synopsis[:2000] + "..."

        return synopsis

    def _clean_value(self, value: str) -> str:
        """
        Clean extracted value by removing wiki markup and HTML.
        """
        value = value.strip()

        # Remove wiki links [[Link|Text]] -> Text or [[Link]] -> Link
        value = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", value)

        # Remove HTML tags
        value = re.sub(r"<[^>]+>", "", value)

        # Remove references
        value = re.sub(r"<ref[^>]*>.*?</ref>", "", value, flags=re.DOTALL)
        value = re.sub(r"<ref[^>]*/?>", "", value)

        # Remove wiki formatting
        value = re.sub(r"'{2,}", "", value)

        # Clean multiple spaces
        value = re.sub(r"\s+", " ", value)

        return value.strip()

    def _parse_list(self, text: str) -> list:
        """
        Parse a list of items (actors, etc.) from wiki text.
        """
        text = self._clean_value(text)

        # Split by newline, bullets, or commas
        items = re.split(r"\n\*|\n-|<br\s*/?>|,", text)

        # Clean and filter items
        cleaned_items = []
        for item in items:
            item = item.strip()
            item = re.sub(r"^\*+\s*", "", item)
            item = re.sub(r"^-+\s*", "", item)

            if item and len(item) > 1:
                cleaned_items.append(item)

        return cleaned_items[:10]

    def _write_jsonl(self, file: TextIO, data: Dict):
        """
        Write a single JSON object as a line to the file.
        """
        json_line = json.dumps(data, ensure_ascii=False)
        file.write(json_line + "\n")


def _movie_document(movie: dict[str, Any]) -> tuple[str | int, str, dict[str, Any]]:
    id_ = f"{movie['title']} {movie['year']}"
    text = movie["synopsis"]
    payload = dict(genre=movie["genre"], duration=movie["duration_minutes"])
    return id_, text, payload


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


def zstd_line_reader(source: str) -> Generator[str, None, None]:
    with open(source, "rb") as f:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(f) as reader:
            text_reader = io.TextIOWrapper(reader, encoding="utf-8")
            for line in text_reader:
                yield line


# =============================================================================
# MAIN SCRIPT
# =============================================================================

if __name__ == "__main__":
    from pathlib import Path

    # ===== STEP 1: EXTRACTION =====
    print("=" * 60)
    print("EXTRACTING FILMS FROM WIKIPEDIA DUMP")
    print("=" * 60 + "\n")

    dump_file = "frwiki-latest-pages-articles.xml"
    output_file = "films_wikipedia.jsonl"

    # Create extractor and start parsing

    if Path(f"{dump_file}.zstd").exists():
        source = zstd_line_reader(f"{dump_file}.zstd")
    elif Path(f"{dump_file}.bz2").exists():
        source = bz2.open(f"{dump_file}.bz2", "rt")
    else:
        print(f"⚠️  Dump file not found: {dump_file}")
        print("Download from: https://dumps.wikimedia.org/frwiki/latest/")
        exit(1)
    extractor = WikipediaFilmExtractor(source, output_file)
    extractor.parse_dump()

    print("\n✅ Processing complete!")
