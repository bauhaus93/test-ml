#!/bin/python3

import requests
import re
import itertools
from urllib.parse import urlparse

from url import Url

class DownloadJob:

    def __init__(self, urls):
        self.urls = urls

    def execute(self):
        session = requests.Session()
        return [url.download(session) for url in self.urls]

def extract_urls(self):
    size = 0
    urls = []
    for match in re.findall('href="[a-z0-9.:/]+"', self.content, re.IGNORECASE):
        url = urlparse(match[6:-1])
        urls.append(url)
    return urls
