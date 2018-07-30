#!/bin/python3

import sqlite3

from url import Url

class DBAccessor:

    def __init__(self, db_name):
        self.db_name = db_name
        self.db_conn = sqlite3.connect(self.db_name)
        self.create_tables()

    def create_tables(self):
        c = self.db_conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS url(
                  id INTEGER PRIMARY KEY AUTOINCREMENT,
                  location TEXT,
                  path TEXT,
                  CONSTRAINT unique_loc_path UNIQUE(location, path) ON CONFLICT IGNORE
                  )""")
        c.execute("""CREATE TABLE IF NOT EXISTS url_pending(
                  url_id INTEGER PRIMARY KEY,
                  time_added INTEGER NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                  FOREIGN KEY(url_id) REFERENCES url(id)
                  )""")
        c.execute("""CREATE TABLE IF NOT EXISTS url_active(
                  url_id INTEGER PRIMARY KEY,
                  FOREIGN KEY(url_id) REFERENCES url(id)
                  )""")
        c.execute("""CREATE TABLE IF NOT EXISTS url_visited(
                  url_id INTEGER PRIMARY KEY,
                  time_visited INTEGER NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                  status_code INTEGER NOT NULL,
                  size INTEGER NOT NULL,
                  FOREIGN KEY(url_id) REFERENCES url(id)
                  )""")

        c.execute("""CREATE TRIGGER IF NOT EXISTS url_insert_trigger
                  AFTER INSERT ON url
                  BEGIN
                    INSERT INTO url_pending(url_id) VALUES(NEW.id);
                  END;
                  """)
        c.execute("""CREATE TRIGGER IF NOT EXISTS url_pending_to_active_trigger
                  AFTER INSERT ON url_active
                  BEGIN
                    DELETE FROM url_pending WHERE url_id = NEW.url_id;
                  END;
                  """)
        c.execute("""CREATE TRIGGER IF NOT EXISTS url_active_to_visited_trigger
                  AFTER INSERT ON url_visited
                  BEGIN
                    DELETE FROM url_active WHERE url_id = NEW.url_id;
                  END;
                  """)
        self.db_conn.commit()

    def add_url(self, url):
        c = self.db_conn.cursor()
        c.execute("INSERT INTO url(location, path) VALUES(?, ?)", (url.location, url.path))
        self.db_conn.commit()

    def add_urls(self, urls):
        c = self.db_conn.cursor()
        for url in urls:
            c.execute("INSERT INTO url(location, path) VALUES(?, ?)", (url.location, url.path))
        self.db_conn.commit()

    def create_request_job(self, job_size = 50):
        c = self.db_conn.cursor()
        c.execute("SELECT id, location, path FROM url_pending INNER JOIN url ON url_pending.url_id = url.id ORDER BY time_added ASC")
        result = list(map(Url._make, c.fetchmany(job_size)))
        ids = list(map(lambda url: (url.id,), result))
        self.set_urls_active(ids)
        return result

    def set_urls_active(self, url_ids):
        c = self.db_conn.cursor()
        c.executemany("INSERT INTO url_active(url_id) VALUES(?)", url_ids)
        self.db_conn.commit()

    def set_urls_visited(self, urls):
        c = self.db_conn.cursor()
        c.executemany("INSERT INTO url_visited(url_id, status_code, size) VALUES(?, ?, ?)", urls)
        self.db_conn.commit()

    def get_pending_urls(self, limit = 50):
        c = self.db_conn.cursor()
        c.execute('SELECT * FROM url_pending ORDER BY time_added ASC')
        return c.fetchmany(limit)

    def has_pending_urls(self):
        return self.count_pending_urls() > 0

    def count_pending_urls(self):
        c = self.db_conn.cursor()
        c.execute('SELECT COUNT(*) FROM url_pending')
        return c.fetchone()[0]

    def count_visited_urls(self):
        c = self.db_conn.cursor()
        c.execute('SELECT COUNT(*) FROM url_visited')
        return c.fetchone()[0]

    def count_active_urls(self):
        c = self.db_conn.cursor()
        c.execute('SELECT COUNT(*) FROM url_active')
        return c.fetchone()[0]
