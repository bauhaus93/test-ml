from raw_content import RawContent

class Url:

    def __init__(self, id = 0, scheme = "http", location = "", path = ""):
        self.id = id
        self.scheme = scheme
        self.location = location
        if len(path) == 0:
            path = '/'
        elif path[0] != '/':
            path = '/' + path
        self.path = path

    def download(self, session):
        url_str = "{0}://{1}{2}".format(self.scheme, self.location, self.path)
        try:
            result = session.get(url_str,
                                 timeout = 5.0,
                                 stream = True,
                                 allow_redirects = True)
        except Exception as exc:
            print("Download from {0} failed: {1}".format(url_str, exc))
            return None
        if should_download(result):
            return RawContent(self.id, result.status_code, str(result.text))
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
