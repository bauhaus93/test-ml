#!/bin/python3

class Page:

    def __init__(self, location, path):
        self.location = location
        self.path = path
        self.status_code = None
        self.content = None

    def download(self, session):
        url_str = "http://{0}{1}".format(self.location, self.path)
        result = session.get(url_str,
                             timeout = 3.0,
                             stream = True,
                             allow_redirects = True)
        if should_download(result):
            size, urls = extract_urls(url, result)
            self.status_code = result.status_code
            self.content = str(result.content)

    def extract_urls(self):
        size = 0
        urls = []
        for match in re.findall('href="[a-z0-9.:/]+"', self.content, re.IGNORECASE):
            url = urlparse(match[6:-1])
            urls.append(url)
        return urls

def should_download(result):
    if result.status_code != 200:
        return False
    if 'Content-Type' in result.headers:
        if not 'text' in result.headers['Content-Type']:
            return False
    else:
        return (result.status_code, None)
    if 'Content-Length' in result.headers:
        if int(result.headers['Content-Length']) > 3e6:
            return False
    else:
        return False
    return True
