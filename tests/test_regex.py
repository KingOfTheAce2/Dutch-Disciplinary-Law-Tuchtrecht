import os, sys; sys.path.append(os.path.dirname(os.path.dirname(__file__)))
import re
from main import TuchtrechtCrawler

def test_xml_pattern():
    pattern = TuchtrechtCrawler.XML_PATTERN
    assert pattern.match('/frbr/tuchtrecht/1994/ECLI-ABC/ocrxml')
    assert not pattern.match('/frbr/tuchtrecht/1994/ECLI-ABC')
