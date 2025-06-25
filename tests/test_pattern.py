import os, sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import re
from main import strip_xml

DOCUMENT_PATTERN = re.compile(r'^/frbr/tuchtrecht/\d{4}/[^/]+$')


def test_document_pattern():
    assert DOCUMENT_PATTERN.match('/frbr/tuchtrecht/1994/ECLI-ABC')
    assert not DOCUMENT_PATTERN.match('/frbr/tuchtrecht/1994/ECLI-ABC/extra')


def test_strip_xml():
    xml = b'<root><p>Some text</p><p>Aldus gegeven door mr. X, voorzitter w.g.</p></root>'
    text = strip_xml(xml)
    assert text == 'Some text Aldus gegeven door mr. X, voorzitter w.g.'
