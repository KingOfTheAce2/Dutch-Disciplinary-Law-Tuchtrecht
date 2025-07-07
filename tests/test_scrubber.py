import pytest

from crawler.scrubber import (
    scrub_title_names,
    scrub_party_names,
    scrub_courtesy_names,
    scrub_gemachtigde_names,
)


@pytest.mark.parametrize(
    "input_text,expected",
    [
        ("De klacht is ingediend door mr. Jansen.", "De klacht is ingediend door mr. NAAM"),
        ("Volgens prof. dr. P. de Vries is dit nodig.", "Volgens prof. NAAM Vries is dit nodig."),
        ("Geen titel hier", "Geen titel hier"),
    ],
)
def test_scrub_title_names(input_text, expected):
    assert scrub_title_names(input_text) == expected


@pytest.mark.parametrize(
    "input_text,expected",
    [
        ("Volgens klager J. Jansen is dat zo.", "Volgens klager NAAM is dat zo."),
        ("De verweerder P. Pieters reageerde niet.", "De verweerder NAAM reageerde niet."),
        ("klager zonder naam", "klager NAAM naam"),
    ],
)
def test_scrub_party_names(input_text, expected):
    assert scrub_party_names(input_text) == expected


@pytest.mark.parametrize(
    "input_text,expected",
    [
        ("De heer Jansen verscheen.", "De heer NAAM"),
        ("Mevrouw M. de Vries was afwezig.", "Mevrouw NAAM afwezig."),
        ("Zonder aanspreektitel", "Zonder aanspreektitel"),
    ],
)
def test_scrub_courtesy_names(input_text, expected):
    assert scrub_courtesy_names(input_text) == expected


@pytest.mark.parametrize(
    "input_text,expected",
    [
        ("De gemachtigde mr. Pieters trad op.", "De gemachtigde mr. NAAM"),
        ("gemachtigde Van der Zee", "gemachtigde NAAM"),
        ("zonder gemachtigde", "zonder gemachtigde"),
    ],
)
def test_scrub_gemachtigde_names(input_text, expected):
    assert scrub_gemachtigde_names(input_text) == expected
