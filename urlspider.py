import scrapy

class GeneratorsSpider(scrapy.Spider):
    name = 'url'
    allowed_domains = ['surplusrecord.com']
    start_urls = ['https://surplusrecord.com/electrical-power/60-hz-single-phase-transformers/']

    def parse(self, response):
        # Extract all links from the page
        for link in response.xpath('/html/body/div[1]/div[2]/div/article/div/div/div[4]/div[51]/div[2]/div[1]').getall():
            yield {'url': link}
        
        # Follow pagination link, if available
        # next_page = response.css('a.page-numbers::attr(href)').get()
        # if next_page is not None:
        #     yield response.follow(next_page, self.parse)