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
        return [(url, url.download(session)) for url in self.urls]
