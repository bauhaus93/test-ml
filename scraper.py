#!/bin/python3

import concurrent.futures
import http.client
import sqlite3
import threading
import time
from urllib.parse import urlparse

from db_accessor import DBAccessor
from manager_thread import ManagerThread
from url import Url

class Scraper:
    def __init__(self, db_name, max_workers = 10):
        self.db_name = db_name
        self.max_workers = 10
        self.db = DBAccessor(db_name)

    def start(self):
        self.managerThread = ManagerThread(self.db_name, self.max_workers)
        self.managerThread.start()

    def stop(self):
        self.managerThread.stop_manager()

    def add_url(self, url):
        parsedUrl = urlparse(url)
        self.db.add_url(Url(None, parsedUrl.netloc, parsedUrl.path))

    def print_stats(self):
        print("Pending: {0}, active: {1}, visited: {2}".format(self.db.count_pending_urls(),
                                                               self.db.count_active_urls(),
                                                               self.db.count_visited_urls()))

if __name__ == '__main__':
    scraper = Scraper("scrape.db")
    scraper.add_url("http://orf.at/")
    scraper.start()
    while True:
        try:
            scraper.print_stats()
            time.sleep(1)
        except KeyboardInterrupt:
            break
    scraper.stop()
