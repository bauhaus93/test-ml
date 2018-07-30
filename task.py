#!/bin/python3

import requests
import re
import itertools
from urllib.parse import urlparse
from collections import namedtuple

from url import Url

DownloadResult = namedtuple('DownloadResult', 'source, status_code, size, urls')

class Task:

    def __init__(self, source_urls):
        self.source_urls = source_urls

    def execute(self):
        session = requests.Session()
        self.results = [download_url(session, url) for url in self.source_urls]

    def persist(self, accessor):
        visited_urls = [(r.source.id, r.status_code, r.size) for r in self.results]
        accessor.set_urls_visited(visited_urls)
        urls = []
        for r in self.results:
            if r.urls:
                urls += r.urls
        found_urls = map(lambda url: Url(None, url.netloc, url.path), urls)
        accessor.add_urls(found_urls)

def extract_urls(url, result):
    size = 0
    urls = []
    for chunk in result.iter_content(chunk_size = int(100e3)):
        if chunk:
            for match in re.findall('href="[a-z0-9.:/]+"', chunk.decode('utf-8'), re.IGNORECASE):
                url = urlparse(match[6:-1])
                urls.append(url)
            size += len(chunk)
    return size, urls
