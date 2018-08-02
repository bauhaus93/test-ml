#!/bin/python3

import re

from urllib.parse import urlparse

from raw_content import RawContent
from url import Url

def extract_urls(source_url, raw_content):
    urls = []
    for match in re.findall('href="[a-z0-9.:/]+"', raw_content.content, re.IGNORECASE):
        parsed = urlparse(match[6:-1])
        if len(parsed.netloc) == 0:
            url = Url(location = source_url.location, path = parsed.path)
        else:
            url = Url(location = parsed.netloc, path = parsed.path)

        urls.append(url)
    return urls
