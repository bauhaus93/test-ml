import re

from bs4 import BeautifulSoup

from urllib.parse import urlparse

from raw_content import RawContent
from url import Url

def extract_urls(source_url, raw_content):
    urls = []
    soup = BeautifulSoup(raw_content.content, features="html.parser")
    for link in soup.find_all("a"):
        parsed = urlparse(link.get("href"))
        scheme = source_url.scheme
        location = source_url.location
        if len(parsed.scheme) > 0:
            scheme = parsed.scheme
        if len(parsed.netloc) > 0:
            location = parsed.netloc
        if scheme == "http" or scheme == "https":
            url = Url(scheme = scheme, location = location, path = parsed.path)
            urls.append(url)

    return urls

def extract_content(raw_content):
    soup = BeautifulSoup(raw_content.content, "html5lib")
    text = [text for text in soup.stripped_strings]
    regex = re.compile(r'[\w\.\?\-! „“,]+', re.UNICODE)
    full_text = ""
    for block in text:
        bare = block.replace(r"\n", "")
        if regex.fullmatch(bare) and len(bare) > 50:
            full_text = full_text + "\n" + bare
    if len(full_text) < 500:
        return None
    return full_text[1:]
