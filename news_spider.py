import scrapy
import csv
import urllib

class NewsSpider(scrapy.Spider):
    name = "NewsSpider"
    scrape_counter = 0
    custom_settings = {
        'LOG_LEVEL': 'INFO' }

    def __init__(self, target_domain, text_parser, scrape_max = 1000, **kwargs):
        super().__init__(**kwargs)
        self.start_urls = ["http://{0}/".format(target_domain)]
        self.allowed_domains = [target_domain]
        self.scrape_max = scrape_max
        self.text_parser = text_parser

    def parse(self, response):
        body = response.xpath('//body')
        if body:
            text = self.text_parser(body)
            url = urllib.parse.urlparse(response.url)
            if len(text) > 500:
                with open('scrape.csv', 'a', newline='') as file:
                    writer = csv.writer(file, delimiter = ',', quotechar = '"', quoting=csv.QUOTE_MINIMAL)
                    writer.writerow([url.netloc, text])
                self.scrape_counter += 1
                if self.scrape_counter % 10 == 0:
                    self.logger.info("scrape count of {0}: {1}".format(self.allowed_domains[0], self.scrape_counter))

        for next_page in response.xpath('//a/@href').extract():
            if "pdf" not in next_page and "mailto" not in next_page:
                if self.scrape_counter < self.scrape_max:
                    yield response.follow(next_page, self.parse)
