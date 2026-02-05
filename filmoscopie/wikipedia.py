import bz2
import json
import re
from typing import Optional, Dict, TextIO
from lxml import etree
from pathlib import Path


class WikipediaFilmExtractorIncremental:
    def __init__(self, dump_path: str, output_path: str):
        self.dump_path = dump_path
        self.output_path = output_path
        self.films_count = 0
        self.pages_processed = 0

        # Namespace XML de Wikipedia
        self.ns = {"mw": "http://www.mediawiki.org/xml/export-0.10/"}

    def parse_dump(self):
        """Parse le dump de manière incrémentale et écrit en JSON Lines"""

        print(f"Ouverture du dump: {self.dump_path}")

        # Ouvrir le fichier d'entrée (bz2 ou non)
        if self.dump_path.endswith(".bz2"):
            input_file = bz2.open(self.dump_path, "rb")
        else:
            input_file = open(self.dump_path, "rb")

        # Ouvrir le fichier de sortie en mode append
        with open(self.output_path, "w", encoding="utf-8") as output_file:
            # Parser incrémental avec iterparse
            context = etree.iterparse(
                input_file,
                events=(
                    "start",
                    "end",
                ),
                tag="{http://www.mediawiki.org/xml/export-0.10/}page",
            )

            print("Début du parsing incrémental...\n")

            for event, elem in context:
                self.pages_processed += 1

                # Traiter la page
                film_data = self._process_page(elem)

                # Si c'est un film, l'écrire immédiatement
                if film_data:
                    self._write_jsonl(output_file, film_data)
                    self.films_count += 1

                    # Afficher la progression tous les 100 films
                    if self.films_count % 100 == 0:
                        print(
                            f"✓ {self.films_count} films extraits "
                            f"({self.pages_processed} pages traitées)"
                        )

                # CRUCIAL: Libérer la mémoire
                elem.clear()
                # Nettoyer aussi les parents pour éviter les fuites mémoire
                while elem.getprevious() is not None:
                    del elem.getparent()[0]

            # Nettoyer le contexte
            del context

        input_file.close()

        print(f"\n{'='*60}")
        print(f"Extraction terminée!")
        print(f"  - Pages traitées: {self.pages_processed:,}")
        print(f"  - Films extraits: {self.films_count:,}")
        print(f"  - Fichier de sortie: {self.output_path}")
        print(f"{'='*60}")

    def _process_page(self, page_elem) -> Optional[Dict]:
        """Traite un élément page et retourne les données du film si applicable"""

        # Extraire le titre
        title_elem = page_elem.find("mw:title", self.ns)
        if title_elem is None or title_elem.text is None:
            return None

        title = title_elem.text

        # Extraire le contenu
        revision = page_elem.find("mw:revision", self.ns)
        if revision is None:
            return None

        text_elem = revision.find("mw:text", self.ns)
        if text_elem is None or text_elem.text is None:
            return None

        content = text_elem.text

        # Vérifier si c'est un film
        if not self._is_film_article(content):
            return None

        # Extraire les données du film
        return self._extract_film_data(title, content)

    def _is_film_article(self, content: str) -> bool:
        """Détecte si l'article concerne un film"""
        # Recherche d'infobox cinéma (case-insensitive)
        infobox_patterns = [
            r"\{\{Infobox Cinéma",
            r"\{\{Infobox Film",
            r"\{\{Infobox film",
        ]
        return any(
            re.search(pattern, content, re.IGNORECASE) for pattern in infobox_patterns
        )

    def _extract_film_data(self, title: str, content: str) -> Dict:
        """Extrait les données structurées du film"""

        film_data = {
            "titre": title,
            "titre_original": None,
            "realisateur": None,
            "annee": None,
            "pays": None,
            "genre": None,
            "duree": None,
            "acteurs": [],
            "scenariste": None,
            "producteur": None,
            "budget": None,
        }

        # Trouver l'infobox
        infobox_match = re.search(
            r"\{\{Infobox[^}]*?(Cinéma|Film|film)\s*\|?(.*?)\n\}\}",
            content,
            re.DOTALL | re.IGNORECASE,
        )

        if not infobox_match:
            return film_data

        infobox_content = infobox_match.group(2)

        # Extraction des champs
        field_patterns = {
            "titre_original": r"titre original\s*=\s*(.+)",
            "realisateur": r"réalisation\s*=\s*(.+)",
            "scenariste": r"scénario\s*=\s*(.+)",
            "producteur": r"producteur\s*=\s*(.+)",
            "pays": r"pays\s*=\s*(.+)",
            "genre": r"genre\s*=\s*(.+)",
            "budget": r"budget\s*=\s*(.+)",
        }

        for field, pattern in field_patterns.items():
            match = re.search(pattern, infobox_content, re.IGNORECASE)
            if match:
                film_data[field] = self._clean_value(match.group(1))

        # Année (extraction spécifique)
        annee_match = re.search(r"année\s*=\s*(\d{4})", infobox_content, re.IGNORECASE)
        if annee_match:
            film_data["annee"] = int(annee_match.group(1))

        # Date de sortie alternative
        if not film_data["annee"]:
            date_match = re.search(
                r"(?:sortie|date)\s*=.*?(\d{4})", infobox_content, re.IGNORECASE
            )
            if date_match:
                film_data["annee"] = int(date_match.group(1))

        # Durée
        duree_match = re.search(r"durée\s*=\s*(\d+)", infobox_content, re.IGNORECASE)
        if duree_match:
            film_data["duree"] = int(duree_match.group(1))

        # Acteurs
        acteurs_match = re.search(
            r"acteur\s*=\s*(.+?)(?:\n\||\n\}\})",
            infobox_content,
            re.IGNORECASE | re.DOTALL,
        )
        if acteurs_match:
            film_data["acteurs"] = self._parse_list(acteurs_match.group(1))

        return film_data

    def _clean_value(self, value: str) -> str:
        """Nettoie une valeur extraite"""
        value = value.strip()

        # Enlever les liens wiki [[Lien|Texte]] -> Texte ou [[Lien]] -> Lien
        value = re.sub(r"\[\[(?:[^|\]]*\|)?([^\]]+)\]\]", r"\1", value)

        # Enlever les balises HTML
        value = re.sub(r"<[^>]+>", "", value)

        # Enlever les refs
        value = re.sub(r"<ref[^>]*>.*?</ref>", "", value, flags=re.DOTALL)
        value = re.sub(r"<ref[^>]*/?>", "", value)

        # Enlever les balises wiki simples
        value = re.sub(r"'{2,}", "", value)

        # Nettoyer les espaces multiples
        value = re.sub(r"\s+", " ", value)

        return value.strip()

    def _parse_list(self, text: str) -> list:
        """Parse une liste d'éléments (acteurs, etc.)"""
        text = self._clean_value(text)

        # Séparer par ligne ou par virgule
        items = re.split(r"\n\*|\n-|<br\s*/?>|,", text)

        # Nettoyer et filtrer
        cleaned_items = []
        for item in items:
            item = item.strip()
            item = re.sub(r"^\*+\s*", "", item)  # Enlever les bullets
            item = re.sub(r"^-+\s*", "", item)
            if item and len(item) > 1:
                cleaned_items.append(item)

        return cleaned_items[:10]  # Limiter à 10 éléments

    def _write_jsonl(self, file: TextIO, data: Dict):
        """Écrit une ligne JSON dans le fichier"""
        json_line = json.dumps(data, ensure_ascii=False)
        file.write(json_line + "\n")


class JSONLinesReader:
    """Utilitaire pour lire le fichier JSON Lines"""

    def __init__(self, filepath: str):
        self.filepath = filepath

    def read_all(self) -> list:
        """Lit tous les films (attention à la mémoire)"""
        films = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                films.append(json.loads(line))
        return films

    def iterate(self):
        """Itère sur les films sans tout charger en mémoire"""
        with open(self.filepath, "r", encoding="utf-8") as f:
            for line in f:
                yield json.loads(line)

    def count(self) -> int:
        """Compte le nombre de films"""
        count = 0
        with open(self.filepath, "r", encoding="utf-8") as f:
            for _ in f:
                count += 1
        return count

    def sample(self, n: int = 10) -> list:
        """Récupère les n premiers films"""
        films = []
        with open(self.filepath, "r", encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= n:
                    break
                films.append(json.loads(line))
        return films

    def filter(self, condition, output_path: str):
        """Filtre les films selon une condition et écrit dans un nouveau fichier"""
        count = 0
        with open(self.filepath, "r", encoding="utf-8") as infile:
            with open(output_path, "w", encoding="utf-8") as outfile:
                for line in infile:
                    film = json.loads(line)
                    if condition(film):
                        outfile.write(line)
                        count += 1
        print(f"Filtré: {count} films écrits dans {output_path}")
        return count

    def to_json(self, output_path: str):
        """Convertit JSON Lines vers JSON standard (array)"""
        films = self.read_all()
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(films, f, ensure_ascii=False, indent=2)
        print(f"Converti en JSON: {output_path}")


# Script d'utilisation principal
if __name__ == "__main__":
    # ===== ÉTAPE 1: Extraction =====
    print("=" * 60)
    print("EXTRACTION DES FILMS DEPUIS LE DUMP WIKIPEDIA")
    print("=" * 60 + "\n")

    dump_file = "frwiki-latest-pages-articles.xml.bz2"
    output_file = "films_wikipedia.jsonl"

    # Vérifier que le dump existe
    if not Path(dump_file).exists():
        print(f"⚠️  Fichier dump non trouvé: {dump_file}")
        print("Téléchargez-le depuis: https://dumps.wikimedia.org/frwiki/latest/")
        print("Exemple: frwiki-latest-pages-articles.xml.bz2")
        exit(1)

    # Créer l'extracteur et lancer le parsing
    extractor = WikipediaFilmExtractorIncremental(dump_file, output_file)
    extractor.parse_dump()

    # ===== ÉTAPE 2: Utilisation du fichier JSON Lines =====
    print("\n" + "=" * 60)
    print("UTILISATION DU FICHIER JSON LINES")
    print("=" * 60 + "\n")

    reader = JSONLinesReader(output_file)

    # Compter les films
    total = reader.count()
    print(f"Total de films dans le fichier: {total:,}\n")

    # Afficher quelques exemples
    print("Exemples de films extraits:")
    print("-" * 60)
    for i, film in enumerate(reader.sample(5), 1):
        print(f"{i}. {film['titre']}")
        if film["annee"]:
            print(f"   Année: {film['annee']}")
        if film["realisateur"]:
            print(f"   Réalisateur: {film['realisateur']}")
        if film["genre"]:
            print(f"   Genre: {film['genre']}")
        print()

    # ===== ÉTAPE 3: Exemples de filtrage =====
    print("=" * 60)
    print("EXEMPLES DE FILTRAGE")
    print("=" * 60 + "\n")

    # Filtrer les films français
    print("1. Films français...")
    reader.filter(
        lambda f: f.get("pays") and "france" in f["pays"].lower(),
        "films_francais.jsonl",
    )

    # Filtrer les films récents (2000+)
    print("\n2. Films après 2000...")
    reader.filter(
        lambda f: f.get("annee") and f["annee"] >= 2000, "films_recents.jsonl"
    )

    # Filtrer les films avec acteurs
    print("\n3. Films avec acteurs renseignés...")
    reader.filter(
        lambda f: f.get("acteurs") and len(f["acteurs"]) > 0, "films_avec_acteurs.jsonl"
    )

    # ===== ÉTAPE 4: Conversion en JSON standard =====
    print("\n" + "=" * 60)
    print("CONVERSION EN JSON STANDARD (optionnel)")
    print("=" * 60 + "\n")

    # Convertir seulement les films récents (pour ne pas surcharger la RAM)
    recent_reader = JSONLinesReader("films_recents.jsonl")
    recent_reader.to_json("films_recents.json")

    print("\n✅ Traitement terminé!")
