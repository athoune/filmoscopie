from io import StringIO
import re
from typing import Any, Optional


def is_draft(text: str) -> bool:
    return text.find("{{ébauche|film") != -1


def is_sub_theme(text: str) -> bool:
    for theme in [
        "projecteur",
        "festival",
        "technologie",
        "studio",
        "caméra",
        "série de films",
    ]:
        if text.find(f"{{Infobox Cinéma ({theme})") != -1:
            return True
    return False


def is_film_article(text: str) -> bool:
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
    return any(re.search(pattern, text, re.IGNORECASE) for pattern in infobox_patterns)


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


def extract_film_data(title: str, text: str) -> dict[str, Any]:
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
    # FIXME handle 'série de films'
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

    to_split = ["writer", "producer", "country", "genre"]

    for field, pattern in field_patterns.items():
        match = re.search(pattern, infobox_content, re.IGNORECASE)
        if match:
            if field in to_split:
                film_data[field] = parse_list(match.group(1))
            else:
                film_data[field] = clean_value(match.group(1))

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
    duration_match = re.search(r"durée\s*=.*?(\d+)", infobox_content, re.IGNORECASE)
    if duration_match:
        film_data["duration_minutes"] = int(duration_match.group(1))

    # ===== EXTRACT ACTORS =====
    actors_match = re.search(
        r"acteur\s*=\s*(.+?)(?:\n\||\n\}\})",
        infobox_content,
        re.IGNORECASE | re.DOTALL,
    )
    if actors_match:
        film_data["actors"] = parse_list(actors_match.group(1))

    # ===== EXTRACT ENGLISH TITLE =====
    # Method 1: Look for "titre anglais" field in infobox
    english_title_match = re.search(
        r"titre anglais\s*=\s*(.+)", infobox_content, re.IGNORECASE
    )
    if english_title_match:
        film_data["english_title"] = clean_value(english_title_match.group(1))

    # Method 2: Look for interlanguage links (less reliable)
    if not film_data["english_title"]:
        # Try to find {{Titre en langue|en|English Title}}
        lang_title_match = re.search(
            r"\{\{Titre en langue\|en\|([^}]+)\}\}", text, re.IGNORECASE
        )
        if lang_title_match:
            film_data["english_title"] = clean_value(lang_title_match.group(1))

    # ===== EXTRACT IMDB ID =====
    # Method 1: Look for IMDb template {{IMDb titre|id=...}}
    imdb_template_match = re.search(
        r"\{\{IMDb\s+titre\s*\|\s*(?:id\s*=\s*)?([a-z]*\d+)", text, re.IGNORECASE
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
    film_data["synopsis"] = extract_synopsis(text)

    return film_data


def extract_synopsis(text: str) -> Optional[str]:
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
            synopsis = clean_synopsis(synopsis)

            # Only return if we have substantial content (at least 50 chars)
            if len(synopsis) >= 50:
                return synopsis

    return None


def clean_synopsis(synopsis: str) -> str:
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


def clean_value(value: str) -> str:
    """
    Clean extracted value by removing wiki markup and HTML.
    """
    value = value.strip()

    # Remove wiki links [[Link|Text]] -> Text or [[Link]] -> Link
    value = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", value)
    value = re.sub(r"\{\{(?:[^|\}]*\|)?([^\}]+)\}\}", r"\1", value)

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


def parse_list(text: str | None) -> list[str]:
    """
    Parse a list of items (actors, etc.) from wiki text.
    """
    if text is None:
        return []
    # Split by newline, bullets, or commas
    items = re.split(r"\n\*|\n-|<br\s*/?>|,", text)

    # Clean and filter items
    return [
        clean_value(item).lstrip("- ").strip()
        for item in items
        if item and len(item) > 1
    ]
