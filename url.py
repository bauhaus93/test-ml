#!/bin/python3

from raw_content import RawContent

class Url:

    def __init__(self, id = 0, location = "", path = ""):
        self.id = id
        self.location = location
        if len(path) == 0:
            path = '/'
        elif path[0] != '/':
            path = '/' + path
        self.path = path

    def download(self, session):
        url_str = "http://{0}{1}".format(self.location, self.path)
        result = session.get(url_str,
                             timeout = 3.0,
                             stream = True,
                             allow_redirects = True)
        if should_download(result):
            return RawContent(self.id, result.status_code, str(result.content))
        return None

    def __str__(self):
        return "Url(id = {0}, location = {1}, path = {2})".format(self.id, self.location, self.path)

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
