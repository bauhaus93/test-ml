import concurrent.futures
import http.client
import sqlite3
import threading
import time

from urllib.parse import urlparse

from db_accessor import DBAccessor
from manager_thread import ManagerThread
from url import Url


#TODO enable usage of imported cookies
#     jar = requests.cookies.RequestsCookieJar()
#     jar.set(key, value, secure=secure, domain=domain, rest=rest)

class Scraper:
    def __init__(self, db_name, max_workers = 10):
        self.db_name = db_name
        self.max_workers = max_workers
        self.db = DBAccessor(db_name)

    def start(self):
        self.managerThread = ManagerThread(self.db_name, self.max_workers)
        self.managerThread.start()

    def stop(self):
        self.managerThread.stop_manager()

    def add_url(self, url):
        parsedUrl = urlparse(url)
        self.db.add_url(Url(None, parsedUrl.scheme, parsedUrl.netloc, parsedUrl.path))

    def add_scrape_location(self, location):
        self.db.add_scrape_location(location)

    def print_stats(self):
        print("Pending: {0}, active: {1}, visited: {2}".format(self.db.count_pending_urls(),
                                                               self.db.count_active_urls(),
                                                               self.db.count_visited_urls()))

if __name__ == '__main__':
    scraper = Scraper("scrape.db")
    #scraper.add_scrape_location("orf.at")
    #scraper.add_url("http://orf.at/")
    #scraper.add_scrape_location("oe24.at")
    #scraper.add_url("http://oe24.at")
    scraper.add_scrape_location("derstandard.at")
    scraper.add_url("https://derstandard.at/")
    scraper.start()
    while True:
        try:
            scraper.print_stats()
            time.sleep(1)
        except KeyboardInterrupt:
            break
    scraper.stop()
