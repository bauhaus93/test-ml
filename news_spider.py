import scrapy
import csv
import urllib

class NewsSpider(scrapy.Spider):
    name = "news"
    allowed_domains = ["orf.at"]
    start_urls = ["http://orf.at/"]
    custom_settings = {
        'LOG_LEVEL': 'INFO',
    }
    def parse(self, response):
        for sel in response.xpath('//body'):
            full_text = ""
            for text in sel.xpath('//div[@id="ss-storyText"]/*/text()').extract():
                if len(text) > 100:
                    full_text = full_text + "\n" + text
            full_text = full_text[1:]

            if len(full_text) > 500:
                with open('orf.csv', 'a', newline='') as file:
                    writer = csv.writer(file, delimiter = ',', quotechar = '"', quoting=csv.QUOTE_MINIMAL)
                    url = urllib.parse.urlparse(response.url)
                    writer.writerow([url.netloc, full_text])

            for next_page in sel.xpath('//a/@href').extract():
                if "pdf" not in next_page:
                    yield response.follow(next_page, self.parse)
