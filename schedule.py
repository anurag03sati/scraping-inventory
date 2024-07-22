import datetime
import os
import subprocess  # Import subprocess module for executing external scripts
from twisted.internet import reactor
from twisted.internet.task import deferLater
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.settings import Settings
from Machinio_Urls import BusinessSpider
from Machinio_data import MySpider


def sleep(secs):
    """Non-blocking sleep callback."""
    return deferLater(reactor, secs, lambda: None)


def run_dataspider():
    """Run DataSpider to crawl additional data."""
    configure_logging()

    # Generate unique filename for DataSpider output
    timestamp = datetime.datetime.now().strftime('%H_%M_%S')
    output_filename_data = f'data_{timestamp}.json'

    # Manually define settings for DataSpider
    settings_data = Settings()
    settings_data.set('FEEDS', {
        output_filename_data: {
            'format': 'json',
            'encoding': 'utf8',
            'store_empty': False,
        },
    })

    # Initialize the runner with the settings for DataSpider
    runner = CrawlerRunner(settings_data)

    # Crawl DataSpider
    d = runner.crawl(MySpider)

    # Add callback to execute to_csv.py after data.json is created
    d.addCallback(lambda _: execute_to_csv(output_filename_data))

    return d


def execute_to_csv(json_filename):
    """Execute to_csv.py script."""
    # Assuming to_csv.py accepts the JSON filename as an argument
    subprocess.run(['python', 'to_csv.py', json_filename])


def run_product_spider():
    """Run ProductSpider to crawl initial URLs."""
    # Generate unique filename for ProductSpider output
    timestamp = datetime.datetime.now().strftime('%H_%M_%S')
    output_filename_url = f'url_{timestamp}.json'

    # Manually define settings for ProductSpider
    settings_url = Settings()
    settings_url.set('FEEDS', {
        output_filename_url: {
            'format': 'json',
            'encoding': 'utf8',
            'store_empty': False,
        },
    })

    # Initialize the runner with the settings for ProductSpider
    runner = CrawlerRunner(settings_url)

    # Crawl ProductSpider
    d = runner.crawl(BusinessSpider)

    # Add callback to run DataSpider after ProductSpider completes
    d.addCallback(lambda _: run_dataspider())

    return d


def schedule_spiders():
    """Schedule running spiders every 30 seconds."""
    d = run_product_spider()

    # Add callback for sleep and scheduling next run
    d.addCallback(lambda _: sleep(600))
    d.addCallback(lambda _: schedule_spiders())


if __name__ == "__main__":
    # Configure logging
    configure_logging()

    # Start scheduling spiders
    schedule_spiders()

    # Start the reactor
    reactor.run()
