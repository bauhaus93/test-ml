#!/bin/python3

import concurrent.futures
import threading
import time
import requests
import re
from urllib.parse import urlparse
from collections import namedtuple

from db_accessor import DBAccessor
from task import Task

def handle_job(urls):
    task = Task(urls)
    task.execute()
    return task

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
                futures = self.add_new_jobs(executor, futures)
                time.sleep(0.5)

    def stop_manager(self):
        self.stop = True
        self.join()

    def add_new_jobs(self, executor, futures):
        while self.db.has_pending_urls() and len(futures) < self.max_workers:
            urls = self.db.create_request_job()
            print('Adding new task, size = ', len(urls))
            futures.append(executor.submit(handle_job, urls))
        return futures

    def handle_finished_jobs(self, executor, futures):
        for fut in concurrent.futures.as_completed(futures):
            try:
                result = fut.result()
            except Exception as exc:
                print("Future generated exception: {0}".format(exc))
            else:
                print('Persisting results of task')
                result.persist(self.db)
        return [fut for fut in futures if not fut.done()]
