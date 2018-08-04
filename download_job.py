import requests
import re
import itertools
from urllib.parse import urlparse

from url import Url
from result import DownloadResult

class DownloadJob:

    def __init__(self, urls):
        self.urls = urls

    def execute(self):
        session = requests.Session()
        return [DownloadResult(source_url = url, raw_content = url.download(session)) for url in self.urls]
