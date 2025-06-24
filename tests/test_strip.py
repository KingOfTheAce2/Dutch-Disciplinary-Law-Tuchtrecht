import os, sys; sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from crawler_base import BaseCrawler

def test_strip_xml_removes_names():
    crawler = BaseCrawler('https://example.com')
    xml = b'<root><p>Some text</p><p>Aldus gegeven door mr. X, voorzitter w.g.</p></root>'
    text = crawler.strip_xml(xml)
    assert 'Aldus' not in text
