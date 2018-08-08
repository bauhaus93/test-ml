import scrapy
from scrapy.crawler import CrawlerProcess

import text_extraction
from news_spider import NewsSpider


proc = CrawlerProcess()
proc.crawl(NewsSpider, target_domain = "orf.at", text_parser = text_extraction.extract_orf)
proc.crawl(NewsSpider, target_domain = "kurier.at", text_parser = text_extraction.extract_kurier)
proc.start()
