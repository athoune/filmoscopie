import bz2
import json
import re
from pathlib import Path
from typing import Dict, Optional, TextIO, Any

from wiki_dump_reader import Cleaner, iterate


class WikipediaFilmExtractor:
    """
    Extracts film data from French Wikipedia dump and writes to JSON Lines format.
    Uses wiki-dump-reader for efficient parsing without loading everything in RAM.
    """

    def __init__(self, dump_path: str, output_path: str):
        self.dump_path = dump_path
        self.output_path = output_path
        self.films_count = 0
        self.pages_processed = 0

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

        # Open output file in write mode
        with open(self.output_path, "w", encoding="utf-8") as output_file:
            # Iterate through pages in the dump
            # wiki-dump-reader handles decompression and parsing automatically
            if self.dump_path.endswith(".bz2"):
                reader = bz2.open(self.dump_path, "rt")
            else:
                reader = open(self.dump_path, "r")

            for title, text in iterate(reader):
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

        print(f"\n{'='*60}")
        print("Extraction complete!")
        print(f"  - Pages processed: {self.pages_processed:,}")
        print(f"  - Films extracted: {self.films_count:,}")
        print(f"  - Output file: {self.output_path}")
        print(f"{'='*60}")

    def _process_page(self, title: str, text: str) -> Optional[dict[str, Any]]:
        """
        Process a single Wikipedia page and extract film data if applicable.

        Args:
            title: Page title
            text: Page wikitext content

        Returns:
            Dictionary with film data or None if not a film article
        """
        # Check if this is a film article
        if not self._is_film_article(text):
            return None

        # Extract structured data from the film
        return self._extract_film_data(title, text)

    def _is_film_article(self, text: str) -> bool:
        """
        Detect if the article is about a film by looking for film infoboxes.

        French Wikipedia uses various infobox templates for films:
        - {{Infobox Cinéma...
        - {{Infobox Film...
        - {{Infobox film...
        """
        infobox_patterns = [
            r"\{\{Infobox Cinéma",
            r"\{\{Infobox Film",
            r"\{\{Infobox film",
        ]

        return any(
            re.search(pattern, text, re.IGNORECASE) for pattern in infobox_patterns
        )

    def _extract_film_data(self, title: str, text: str) -> dict[str, Any]:
        """
        Extract structured data from a film article.

        Parses the infobox to extract:
        - Original title
        - Director
        - Year
        - Country
        - Genre
        - Duration
        - Actors
        - Writer
        - Producer
        - Budget

        Args:
            title: Film title (page title)
            text: Article wikitext

        Returns:
            Dictionary with extracted film data
        """
        film_data = {
            "title": title,
            "original_title": None,
            "director": None,
            "year": None,
            "country": None,
            "genre": None,
            "duration_minutes": None,
            "actors": [],
            "writer": None,
            "producer": None,
            "budget": None,
        }

        # Find the infobox using regex
        # Matches {{Infobox ... | ... }}
        infobox_match = re.search(
            r"\{\{Infobox[^}]*?(Cinéma|Film|film)\s*\|?(.*?)\n\}\}",
            text,
            re.DOTALL | re.IGNORECASE,
        )

        if not infobox_match:
            return film_data

        infobox_content = infobox_match.group(2)

        # Define field patterns to extract from infobox
        # French field names -> English keys
        field_patterns = {
            "original_title": r"titre original\s*=\s*(.+)",
            "director": r"réalisation\s*=\s*(.+)",
            "writer": r"scénario\s*=\s*(.+)",
            "producer": r"(?:producteur|production)\s*=\s*(.+)",
            "country": r"pays\s*=\s*(.+)",
            "genre": r"genre\s*=\s*(.+)",
            "budget": r"budget\s*=\s*(.+)",
        }

        # Extract each field
        for field, pattern in field_patterns.items():
            match = re.search(pattern, infobox_content, re.IGNORECASE)
            if match:
                film_data[field] = self._clean_value(match.group(1))

        # Extract year (special handling for integer)
        year_match = re.search(r"année\s*=\s*(\d{4})", infobox_content, re.IGNORECASE)
        if year_match:
            film_data["year"] = int(year_match.group(1))

        # Alternative: extract from release date if year not found
        if not film_data["year"]:
            date_match = re.search(
                r"(?:sortie|date)\s*=.*?(\d{4})", infobox_content, re.IGNORECASE
            )
            if date_match:
                film_data["year"] = int(date_match.group(1))

        # Extract duration in minutes
        duration_match = re.search(r"durée\s*=\s*(\d+)", infobox_content, re.IGNORECASE)
        if duration_match:
            film_data["duration_minutes"] = int(duration_match.group(1))

        # Extract actors list
        actors_match = re.search(
            r"acteur\s*=\s*(.+?)(?:\n\||\n\}\})",
            infobox_content,
            re.IGNORECASE | re.DOTALL,
        )
        if actors_match:
            film_data["actors"] = self._parse_list(actors_match.group(1))

        return film_data

    def _clean_value(self, value: str) -> str:
        """
        Clean extracted value by removing wiki markup and HTML.

        Removes:
        - Wiki links: [[Link|Text]] -> Text
        - HTML tags: <tag>content</tag> -> content
        - References: <ref>...</ref> -> (removed)
        - Wiki formatting: '''bold''' -> bold

        Args:
            value: Raw extracted value

        Returns:
            Cleaned string
        """
        value = value.strip()

        # Remove wiki links [[Link|Text]] -> Text or [[Link]] -> Link
        value = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", value)

        # Remove HTML tags
        value = re.sub(r"<[^>]+>", "", value)

        # Remove references <ref>...</ref>
        value = re.sub(r"<ref[^>]*>.*?</ref>", "", value, flags=re.DOTALL)
        value = re.sub(r"<ref[^>]*/?>", "", value)

        # Remove wiki formatting (bold, italic)
        value = re.sub(r"'{2,}", "", value)

        # Clean multiple spaces
        value = re.sub(r"\s+", " ", value)

        return value.strip()

    def _parse_list(self, text: str) -> list:
        """
        Parse a list of items (actors, etc.) from wiki text.

        Handles:
        - Bullet lists: * Item
        - Line breaks
        - Comma separated values

        Args:
            text: Raw list text

        Returns:
            List of cleaned items (max 10)
        """
        text = self._clean_value(text)

        # Split by newline, bullets, or commas
        items = re.split(r"\n\*|\n-|<br\s*/?>|,", text)

        # Clean and filter items
        cleaned_items = []
        for item in items:
            item = item.strip()
            # Remove leading bullets or dashes
            item = re.sub(r"^\*+\s*", "", item)
            item = re.sub(r"^-+\s*", "", item)

            # Only keep non-empty items
            if item and len(item) > 1:
                cleaned_items.append(item)

        # Limit to 10 items to avoid huge lists
        return cleaned_items[:10]

    def _write_jsonl(self, file: TextIO, data: Dict):
        """
        Write a single JSON object as a line to the file (JSON Lines format).

        Args:
            file: Open file handle
            data: Dictionary to write
        """
        json_line = json.dumps(data, ensure_ascii=False)
        file.write(json_line + "\n")


class JSONLinesReader:
    """
    Utility class to read and manipulate JSON Lines files efficiently.
    Allows streaming processing without loading everything into memory.
    """

    def __init__(self, filepath: str):
        self.filepath = filepath

    def read_all(self) -> list:
        """
        Read all films into memory.
        WARNING: Use only for small files or you'll run out of RAM!

        Returns:
            List of all film dictionaries
        """
        films = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                films.append(json.loads(line))
        return films

    def iterate(self):
        """
        Iterate over films one at a time (memory efficient).

        Yields:
            Film dictionary for each line
        """
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)

    def count(self) -> int:
        """
        Count total number of films in the file.

        Returns:
            Number of lines (films)
        """
        count = 0
        with open(self.filepath, "r", encoding="utf-8") as f:
            for _ in f:
                count += 1
        return count

    def sample(self, n: int = 10) -> list:
        """
        Get the first n films.

        Args:
            n: Number of films to retrieve

        Returns:
            List of first n films
        """
        films = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= n:
                    break
                films.append(json.loads(line))
        return films

    def filter(self, condition, output_path: str):
        """
        Filter films based on a condition and write to new file.

        Args:
            condition: Function that takes a film dict and returns bool
            output_path: Path to write filtered results

        Returns:
            Number of films that matched the condition
        """
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
        """
        Convert JSON Lines to standard JSON array format.
        WARNING: Loads everything into memory!

        Args:
            output_path: Path to write JSON array
        """
        films = self.read_all()
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(films, f, ensure_ascii=False, indent=2)
        print(f"Converted to JSON: {output_path}")


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
        print("Example: frwiki-latest-pages-articles.xml.bz2")
        exit(1)

    # Create extractor and start parsing
    extractor = WikipediaFilmExtractor(dump_file, output_file)
    extractor.parse_dump()

    # ===== STEP 2: USING THE JSON LINES FILE =====
    print("\n" + "=" * 60)
    print("USING THE JSON LINES FILE")
    print("=" * 60 + "\n")

    reader = JSONLinesReader(output_file)

    # Count films
    total = reader.count()
    print(f"Total films in file: {total:,}\n")

    # Display some examples
    print("Sample films extracted:")
    print("-" * 60)
    for i, film in enumerate(reader.sample(5), 1):
        print(f"{i}. {film['title']}")
        if film["year"]:
            print(f"   Year: {film['year']}")
        if film["director"]:
            print(f"   Director: {film['director']}")
        if film["genre"]:
            print(f"   Genre: {film['genre']}")
        print()

    # ===== STEP 3: FILTERING EXAMPLES =====
    print("=" * 60)
    print("FILTERING EXAMPLES")
    print("=" * 60 + "\n")

    # Filter French films
    print("1. French films...")
    reader.filter(
        lambda f: f.get("country") and "france" in f["country"].lower(),
        "films_french.jsonl",
    )

    # Filter recent films (2000+)
    print("\n2. Films after 2000...")
    reader.filter(lambda f: f.get("year") and f["year"] >= 2000, "films_recent.jsonl")

    # Filter films with actors
    print("\n3. Films with actors listed...")
    reader.filter(
        lambda f: f.get("actors") and len(f["actors"]) > 0, "films_with_actors.jsonl"
    )

    # ===== STEP 4: CONVERSION TO STANDARD JSON (OPTIONAL) =====
    print("\n" + "=" * 60)
    print("CONVERSION TO STANDARD JSON (optional)")
    print("=" * 60 + "\n")

    # Convert only recent films to avoid RAM overload
    recent_reader = JSONLinesReader("films_recent.jsonl")
    recent_reader.to_json("films_recent.json")

    print("\n✅ Processing complete!")
