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
