from bs4 import BeautifulSoup

class RawContent:

    def __init__(self, url_id, status_code, content):
        self.url_id = url_id
        self.status_code = status_code
        self.content = content

    def prepare(self):
        pass#self


    def __str__(self):
        return "RawContent(url_id = {0}, status_code = {1}, content_len = {2})".format(self.url_id, self.status_code, len(self.content))
