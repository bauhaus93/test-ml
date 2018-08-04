import sqlite3

from url import Url

from download_job import DownloadJob

class DBAccessor:

    def __init__(self, db_name):
        self.db_name = db_name
        self.db_conn = sqlite3.connect(self.db_name)
        self.create_tables()

    def create_tables(self):
        c = self.db_conn.cursor()
        c.execute("""CREATE TABLE IF NOT EXISTS url(
                  url_id INTEGER PRIMARY KEY AUTOINCREMENT,
                  scheme TEXT NOT NULL DEFAULT 'http',
                  location TEXT,
                  path TEXT,
                  state INTEGER DEFAULT 0 CHECK (state >= 0 AND state <= 2),
                  time_added INTEGER NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                  CONSTRAINT unique_loc_path UNIQUE(location, path) ON CONFLICT IGNORE
                  )""")

        c.execute("""CREATE TABLE IF NOT EXISTS raw_content(
                  url_id INTEGER PRIMARY KEY,
                  time_visited INTEGER NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                  content TEXT,
                  FOREIGN KEY(url_id) REFERENCES url(url_id) ON DELETE CASCADE
                  )""")
        self.db_conn.commit()

    def add_url(self, url):
        c = self.db_conn.cursor()
        c.execute("INSERT INTO url(scheme, location, path) VALUES(?, ?, ?)", (url.scheme, url.location, url.path))
        self.db_conn.commit()

    def add_urls(self, urls):
        c = self.db_conn.cursor()
        for url in urls:
            c.execute("INSERT INTO url(scheme, location, path) VALUES(?, ?, ?)", (url.scheme, url.location, url.path))
        self.db_conn.commit()

    def count_pending_urls(self):
        c = self.db_conn.cursor()
        c.execute("SELECT COUNT(url_id) FROM url WHERE state = 0")
        return c.fetchone()[0]

    def count_active_urls(self):
        c = self.db_conn.cursor()
        c.execute("SELECT COUNT(url_id) FROM url WHERE state = 1")
        return c.fetchone()[0]

    def count_visited_urls(self):
        c = self.db_conn.cursor()
        c.execute("SELECT COUNT(url_id) FROM url WHERE state = 2")
        return c.fetchone()[0]

    def create_download_job(self, limit):
        c = self.db_conn.cursor()
        c.execute('SELECT url_id, scheme, location, path FROM url WHERE state = 0 ORDER BY time_added ASC')
        urls = [Url(url[0], url[1], url[2], url[3]) for url in c.fetchmany(limit)]
        if len(urls) == 0:
            return None
        c.executemany('UPDATE url SET state = 1 WHERE url_id = ?', [(u.id,) for u in urls])
        self.db_conn.commit()
        return DownloadJob(urls)

    def persist_download_job_results(self, results):
        c = self.db_conn.cursor()
        c.executemany('INSERT INTO raw_content(url_id, content) VALUES(?, ?)', [(r[1].url_id, r[1].content) for r in results if not r[1] is None])
        c.executemany('UPDATE url SET state = 2 WHERE url_id = ?', [(r[0].id,) for r in results])
        self.db_conn.commit()
