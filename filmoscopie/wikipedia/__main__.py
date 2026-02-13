from ..wikipedia import WikipediaFilmExtractor, source

# ===== STEP 1: EXTRACTION =====
print("=" * 60)
print("EXTRACTING FILMS FROM WIKIPEDIA DUMP")
print("=" * 60 + "\n")

extractor = WikipediaFilmExtractor(source())
extractor.parse_dump()

print("\n✅ Processing complete!")
