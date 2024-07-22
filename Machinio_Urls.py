
import scrapy
from scrapy import signals
from scrapy.http import HtmlResponse
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import time
from items import MinistryItem

class BusinessSpider(scrapy.Spider):
    name = 'urlspider'

    categories = ['generators', 'mcc', 'breakers', 'switches','fuses', 'dry-type-transformer', 'transformers-oil']
    category_index = 0
    scraped_urls = set()
    
    category_to_field = {
        'generators': 'generators',
        'mcc': 'motor',
        'breakers': 'switchgears',
        'switches': 'switches',
        'fuses':'switches',
        'dry-type-transformer': 'transformer',
        'transformers-oil': 'transformer'
    }

    def __init__(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        self.driver = webdriver.Chrome(options=chrome_options)

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        spider = super(BusinessSpider, cls).from_crawler(crawler, *args, **kwargs)
        crawler.signals.connect(spider.spider_closed, signal=signals.spider_closed)
        return spider

    def spider_closed(self, spider):
        self.driver.quit()

    def start_requests(self):
        category = self.categories[self.category_index]
        url = f'https://www.lenmark.com/collections/{category}'
        yield scrapy.Request(url, callback=self.parse)

    def parse(self, response):
        self.driver.get(response.url)
        
        last_height = self.driver.execute_script("return document.body.scrollHeight")
        while True:
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(4)  
            new_height = self.driver.execute_script("return document.body.scrollHeight")
            if new_height == last_height:
                break
            last_height = new_height

        body = self.driver.page_source
        response = HtmlResponse(url=self.driver.current_url, body=body, encoding='utf-8')
        base_url = 'https://www.lenmark.com/'
        incomplete_urls = response.css('a.product-item__image-wrapper::attr(href)').getall()
        complete_urls = [base_url + url if not url.startswith('http') else url for url in incomplete_urls]

        category = self.categories[self.category_index]
        field_name = self.category_to_field.get(category, category)  

        if not complete_urls:
            self.log(f"No data found for {category}. Skipping this category.")
        else:
            for url in complete_urls:
                if url not in self.scraped_urls:
                    self.scraped_urls.add(url)
                    item = MinistryItem()
                    item[field_name] = url
                    yield item

        self.category_index += 1
        if self.category_index < len(self.categories):
            next_category = self.categories[self.category_index]
            next_url = f'https://www.lenmark.com/collections/{next_category}'
            self.log(f"Scraping next category: {next_category}")
            yield scrapy.Request(next_url, callback=self.parse)
