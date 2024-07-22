import scrapy

class MinistryItem(scrapy.Item):
    transformer = scrapy.Field()
    generators = scrapy.Field()
    switchgears = scrapy.Field()
    switches = scrapy.Field()
    motor =  scrapy.Field()
