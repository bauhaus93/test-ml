import sqlite3

from url import Url

from download_job import DownloadJob
from extractor import extract_urls, extract_content

class DBAccessor:

    def __init__(self, db_name):
        self.db_name = db_name
        self.db_conn = sqlite3.connect(self.db_name)
        self.create_tables()
        self.move_active_to_pending()

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

        c.execute("""CREATE TABLE IF NOT EXISTS content(
                  url_id INTEGER PRIMARY KEY,
                  time_visited INTEGER NOT NULL DEFAULT (DATETIME('now', 'localtime')),
                  content TEXT,
                  FOREIGN KEY(url_id) REFERENCES url(url_id) ON DELETE CASCADE
                  )""")
        c.execute("""CREATE TABLE IF NOT EXISTS scrape_location(
                  location_id INTEGER PRIMARY KEY AUTOINCREMENT,
                  valid_location TEXT UNIQUE NOT NULL
                  )""")

        c.execute("""CREATE TRIGGER IF NOT EXISTS url_filter
                  BEFORE INSERT ON url
                  FOR EACH ROW
                  BEGIN
                    SELECT RAISE(IGNORE)
                    WHERE NOT EXISTS (SELECT 1
                                      FROM scrape_location
                                      WHERE NEW.location LIKE '%' || valid_location);
                  END;
                  """)
        self.db_conn.commit()

    def add_scrape_location(self, location):
        c = self.db_conn.cursor()
        c.execute("INSERT INTO scrape_location(valid_location) VALUES(?)", (location,))
        self.db_conn.commit()

    def add_url(self, url):
        c = self.db_conn.cursor()
        c.execute("INSERT INTO url(scheme, location, path) VALUES(?, ?, ?)", (url.scheme, url.location, url.path))
        self.db_conn.commit()

    def add_urls(self, urls, cursor):
        cursor.executemany("INSERT INTO url(scheme, location, path) VALUES(?, ?, ?)", [(url.scheme, url.location, url.path) for url in urls])

    def set_urls_state(self, urls, state, cursor):
        cursor.executemany('UPDATE url SET state = ? WHERE url_id = ?', [(state, url.id) for url in urls])

    def add_content(self, source_url, content, cursor):
        cursor.execute('INSERT INTO content(url_id, content) VALUES(?, ?)', (source_url.id, content))

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

    def move_active_to_pending(self):
        c = self.db_conn.cursor()
        c.execute("UPDATE url SET state = 0 WHERE state = 1")
        self.db_conn.commit()

    def create_download_job(self, limit):
        c = self.db_conn.cursor()
        c.execute('SELECT url_id, scheme, location, path FROM url WHERE state = 0 ORDER BY time_added ASC')
        urls = [Url(url[0], url[1], url[2], url[3]) for url in c.fetchmany(limit)]
        if len(urls) == 0:
            return None
        c.executemany('UPDATE url SET state = 1 WHERE url_id = ?', [(u.id,) for u in urls])
        self.db_conn.commit()
        return DownloadJob(urls)

    def persist_download_results(self, results):
        cursor = self.db_conn.cursor()
        successful_results = [res for res in results if not res.raw_content is None]
        urls = [extract_urls(res.source_url, res.raw_content) for res in successful_results]
        urls = [item for sublist in urls for item in sublist]
        self.add_urls(urls, cursor)

        for res in successful_results:
            content = extract_content(res.raw_content)
            if content:
                self.add_content(res.source_url, content, cursor)

        self.set_urls_state([res.source_url for res in results], 2, cursor)
        self.db_conn.commit()
