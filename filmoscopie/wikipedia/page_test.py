from pytest import fixture

from .page import is_film_article, is_draft, extract_film_data, parse_list


@fixture
def dark_city() -> str:
    return open("./fixtures/dark_city_fr.wiki", "r").read()


def test_is_film(dark_city):
    assert is_film_article(dark_city)


def test_is_draft(dark_city):
    assert not is_draft(dark_city)


def test_parse_list():
    raw = "[[Lem Dobbs]]<br>[[David S. Goyer]]<br>Alex Proyas"
    assert parse_list(raw) == ["Lem Dobbs", "David S. Goyer", "Alex Proyas"]


def test_extract_film_data(dark_city):
    film = extract_film_data("Dark City", dark_city)
    print(film.keys())
    assert film["title"] == "Dark City"
    assert film["original_title"] == "Dark City"
    # FIXME
    # assert film['english_title'] == "Dark City"
    assert film["director"] == "Alex Proyas"
    assert film["year"] == 1998
    assert film["country"] == ["Australie", "États-Unis"]
    assert film["genre"] == ["science-fiction"]
    # FIXME
    assert film["duration_minutes"] == 95
    assert film["actors"] == [
        "Rufus Sewell",
        "William Hurt",
        "Kiefer Sutherland",
        "Jennifer Connelly",
    ]
    assert film["writer"] == ["Lem Dobbs", "David S. Goyer", "Alex Proyas"]
    assert film["producer"] == ["Mystery Clock Cinema", "New Line Cinema"]
    assert film["budget"] is None
    assert film["imdb_id"] == "0118929"
    # Synopsis has its own test
