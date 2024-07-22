import scrapy
import json
from glob import glob
import os

class MySpider(scrapy.Spider):
    name = 'dataspider'

    def start_requests(self):
        try:
            latest_file = max(glob('url_*.json'), key=os.path.getctime)
            print(latest_file)
            with open(latest_file, 'r') as f:
                urls_data = json.load(f)
        except json.JSONDecodeError as e:
            self.logger.error(f"Error reading JSON file: {e}")
            return

        for url_data in urls_data:
            category = next(iter(url_data))  # Assuming each entry in JSON has only one key
            url = url_data[category]
            yield scrapy.Request(url=url, callback=self.parse, meta={'category': category})

    def parse(self, response):
        category = response.meta['category']

        # Extract manufacturer name
        manufacturer_name = response.css('.link--accented.h4::text').get(default='').strip()

        # Extract product name
        product_name = response.css('.product-meta__title::text').get(default='').strip()

        # Extract specifications
        specs = {}
        rows = response.css('.productpage-td')
        for i in range(0, len(rows), 2):
            key = rows[i].css('strong::text').get(default='').strip()
            value = rows[i + 1].css('::text').get(default='').strip()
            if key and value:
                specs[key] = value

        # Extract other_description
        other_description_parts = response.css('tr.productpage-tr *::text').getall()
        other_description = []
        i = 0
        while i < len(other_description_parts):
            key = other_description_parts[i].strip()
            value = other_description_parts[i + 1].strip() if i + 1 < len(other_description_parts) else None
            if key in specs and value == specs[key]:
                i += 2
            elif key in specs:
                i += 1
            else:
                other_description.append(key)
                if value:
                    other_description.append(value)
                i += 2

        other_description_text = ' '.join(other_description).strip()

        # Yield the extracted data as a dictionary
        yield {
            'category': category,
            'url': response.url,
            'manufacturer': manufacturer_name,
            'title': product_name,
            'specifications': specs,
            'other_description': other_description_text
             
        }
