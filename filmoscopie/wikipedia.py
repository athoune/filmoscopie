import bz2
import json
import re
from io import StringIO
from pathlib import Path
from typing import Dict, Optional, TextIO, Any, Generator

import zstandard as zstd
from wiki_dump_reader import Cleaner, iterate


def titles(txt: str):
    title = re.compile(r"^(=+) (.*?) (=+)$")
    poz = 0
    for i, line in enumerate(StringIO(txt).readlines()):
        m = title.match(line)
        if m is not None:
            assert m[1] == m[3]
            yield i, poz, m[2]
        poz += len(line)


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

        # Initialize wiki cleaner to remove markup
        self.cleaner = Cleaner()

    def parse_dump(self):
        """
        Parse the Wikipedia dump incrementally and write films to JSON Lines.
        Memory efficient: processes one page at a time.
        """
        print(f"Opening dump: {self.dump_path}")
        print(f"Output file: {self.output_path}")
        print("Starting incremental parsing...\n")

        self.draft_writer = open("films_without_draft.txt", "w")
        # Open output file in write mode
        with open(self.output_path, "w", encoding="utf-8") as output_file:
            # Iterate through pages in the dump
            for title, text in iterate(self.dump_path):
                self.pages_processed += 1

                # Process the page
                film_data = self._process_page(title, text)

                # If it's a film, write it immediately to the file
                if film_data:
                    self._write_jsonl(output_file, film_data)
                    self.films_count += 1

                    # Display progress every 100 films
                    if self.films_count % 100 == 0:
                        print(
                            f"✓ {self.films_count} films extracted "
                            f"({self.pages_processed:,} pages processed)"
                        )

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

    def _extract_film_data(self, title: str, text: str) -> Dict:
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
            "title": title,
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


class JSONLinesReader:
    """
    Utility class to read and manipulate JSON Lines files efficiently.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath

    def read_all(self) -> list:
        """Read all films into memory (use carefully!)"""
        films = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                films.append(json.loads(line))
        return films

    def iterate(self):
        """Iterate over films one at a time (memory efficient)"""
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)

    def count(self) -> int:
        """Count total number of films"""
        count = 0
        with open(self.filepath, "r", encoding="utf-8") as f:
            for _ in f:
                count += 1
        return count

    def sample(self, n: int = 10) -> list:
        """Get the first n films"""
        films = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= n:
                    break
                films.append(json.loads(line))
        return films

    def filter(self, condition, output_path: str):
        """Filter films based on condition and write to new file"""
        count = 0
        with open(self.filepath, "r", encoding="utf-8") as infile:
            with open(output_path, "w", encoding="utf-8") as outfile:
                for line in infile:
                    film = json.loads(line)
                    if condition(film):
                        outfile.write(line)
                        count += 1
        print(f"Filtered: {count} films written to {output_path}")
        return count

    def to_json(self, output_path: str):
        """Convert JSON Lines to standard JSON array"""
        films = self.read_all()
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(films, f, ensure_ascii=False, indent=2)
        print(f"Converted to JSON: {output_path}")

    def get_statistics(self):
        """
        Calculate statistics about the extracted data.

        Returns:
            Dictionary with statistics
        """
        stats = {
            "total": 0,
            "with_synopsis": 0,
            "with_imdb_id": 0,
            "with_english_title": 0,
            "with_year": 0,
            "with_director": 0,
            "avg_synopsis_length": 0,
        }

        synopsis_lengths = []

        for film in self.iterate():
            stats["total"] += 1

            if film.get("synopsis"):
                stats["with_synopsis"] += 1
                synopsis_lengths.append(len(film["synopsis"]))

            if film.get("imdb_id"):
                stats["with_imdb_id"] += 1

            if film.get("english_title"):
                stats["with_english_title"] += 1

            if film.get("year"):
                stats["with_year"] += 1

            if film.get("director"):
                stats["with_director"] += 1

        if synopsis_lengths:
            stats["avg_synopsis_length"] = sum(synopsis_lengths) / len(synopsis_lengths)

        return stats


def _movie_document(movie: dict[str, Any]) -> tuple[str | int, str, dict[str, Any]]:
    id_ = f"{movie['title']} {movie['year']}"
    text = movie["synopsis"]
    payload = dict(genre=movie["genre"], duration=movie["duration_minutes"])
    return id_, text, payload


def movies_documents() -> tuple[
    Generator[tuple[int, str, dict[str, Any]], None, None], int
]:
    movies = JSONLinesReader("films_wikipedia.jsonl")

    def _loop():
        i = 0
        for movie in movies.iterate():
            text = movie["synopsis"]
            genre = movie["genre"]
            if genre is None:
                genres = []
            else:
                genres = [a.strip().lower() for a in genre.split((","))]
            m = movie["title"].replace("(film)", "").strip()
            payload = dict(
                title=f"{m} {movie['year']}",
                genre=genres,
                duration=movie["duration_minutes"],
                year=movie["year"],
                imdb=movie["imdb_id"],
            )
            yield i, text, payload
            i += 1

    return (
        _loop(),
        movies.count(),
    )


# =============================================================================
# MAIN SCRIPT
# =============================================================================

if __name__ == "__main__":
    # ===== STEP 1: EXTRACTION =====
    print("=" * 60)
    print("EXTRACTING FILMS FROM WIKIPEDIA DUMP")
    print("=" * 60 + "\n")

    dump_file = "frwiki-latest-pages-articles.xml.bz2"
    output_file = "films_wikipedia.jsonl"

    # Check if dump file exists
    if not Path(dump_file).exists():
        print(f"⚠️  Dump file not found: {dump_file}")
        print("Download from: https://dumps.wikimedia.org/frwiki/latest/")
        exit(1)

    # Create extractor and start parsing
    extractor = WikipediaFilmExtractor(bz2.open(dump_file, "rt"), output_file)
    extractor.parse_dump()

    # ===== STEP 2: DISPLAY SAMPLES =====
    print("\n" + "=" * 60)
    print("SAMPLE EXTRACTED FILMS")
    print("=" * 60 + "\n")

    reader = JSONLinesReader(output_file)

    # Show detailed samples
    for i, film in enumerate(reader.sample(3), 1):
        print(f"Film #{i}: {film['title']}")
        print("-" * 60)

        if film.get("english_title"):
            print(f"English title: {film['english_title']}")

        if film.get("year"):
            print(f"Year: {film['year']}")

        if film.get("director"):
            print(f"Director: {film['director']}")

        if film.get("imdb_id"):
            print(f"IMDb ID: {film['imdb_id']}")
            print(f"IMDb URL: https://www.imdb.com/title/{film['imdb_id']}/")

        if film.get("synopsis"):
            synopsis_preview = film["synopsis"][:200]
            if len(film["synopsis"]) > 200:
                synopsis_preview += "..."
            print(f"\nSynopsis ({len(film['synopsis'])} chars):")
            print(f"  {synopsis_preview}")

        print("\n")

    # ===== STEP 3: STATISTICS =====
    print("=" * 60)
    print("EXTRACTION STATISTICS")
    print("=" * 60 + "\n")

    stats = reader.get_statistics()

    print(f"Total films: {stats['total']:,}")
    # Only show percentages if we have films
    if stats["total"] > 0:
        print("\nData completeness:")
        print(
            f"  - With synopsis: {stats['with_synopsis']:,} ({stats['with_synopsis'] / stats['total'] * 100:.1f}%)"
        )
        print(
            f"  - With IMDb ID: {stats['with_imdb_id']:,} ({stats['with_imdb_id'] / stats['total'] * 100:.1f}%)"
        )
        print(
            f"  - With English title: {stats['with_english_title']:,} ({stats['with_english_title'] / stats['total'] * 100:.1f}%)"
        )
        print(
            f"  - With year: {stats['with_year']:,} ({stats['with_year'] / stats['total'] * 100:.1f}%)"
        )
        print(
            f"  - With director: {stats['with_director']:,} ({stats['with_director'] / stats['total'] * 100:.1f}%)"
        )

    if stats["avg_synopsis_length"] > 0:
        print(
            f"\nAverage synopsis length: {stats['avg_synopsis_length']:.0f} characters"
        )

    # ===== STEP 4: FILTERING EXAMPLES =====
    print("\n" + "=" * 60)
    print("FILTERING EXAMPLES")
    print("=" * 60 + "\n")

    # Films with complete IMDb data
    print("1. Films with IMDb ID...")
    reader.filter(lambda f: f.get("imdb_id") is not None, "films_with_imdb.jsonl")

    # Films with synopsis
    print("\n2. Films with synopsis...")
    reader.filter(lambda f: f.get("synopsis") is not None, "films_with_synopsis.jsonl")

    # Recent films with complete data
    print("\n3. Recent films (2010+) with complete data...")
    reader.filter(
        lambda f: (
            f.get("year")
            and f["year"] >= 2010
            and f.get("synopsis")
            and f.get("imdb_id")
        ),
        "films_recent_complete.jsonl",
    )

    print("\n✅ Processing complete!")
