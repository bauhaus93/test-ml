import concurrent.futures
import threading
import time
import requests
import re
from urllib.parse import urlparse
from collections import namedtuple

from db_accessor import DBAccessor
from download_job import DownloadJob
from url_extract import extract_urls

class ManagerThread(threading.Thread):

    def __init__(self, db_name, max_workers):
        threading.Thread.__init__(self)
        self.db_name = db_name
        self.max_workers = max_workers

    def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers = self.max_workers) as executor:
            self.db = DBAccessor(self.db_name)
            self.stop = False
            futures = []
            while not self.stop:
                futures = self.handle_finished_jobs(executor, futures)
                futures = self.add_download_jobs(executor, futures)
                time.sleep(2)

    def stop_manager(self):
        self.stop = True
        self.join()

    def add_download_jobs(self, executor, futures):
        new_jobs = self.max_workers - len(futures)
        for i in range(new_jobs):
            job = self.db.create_download_job(25)
            if not job is None:
                futures.append(executor.submit(DownloadJob.execute, job))
                print("active jobs: ", len(futures))
            else:
                break
        return futures

    def handle_finished_jobs(self, executor, futures):
        for fut in futures:
            if fut.done():
                try:
                    results = fut.result()
                except Exception as exc:
                    print("Future generated exception:", exc)
                else:
                    found_urls = []
                    for res in results:
                        if not res[1] is None:
                            found_urls += extract_urls(res[0], res[1])
                    self.db.add_urls(found_urls)
                    self.db.persist_download_job_results(results)
        return [fut for fut in futures if not fut.done()]
