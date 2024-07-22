import datetime
import subprocess  # Import subprocess module for executing external scripts
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging
from scrapy.settings import Settings
from Machinio_Urls import BusinessSpider
from Machinio_data import MySpider
import logging

# Configure logging to output to a file
logging.basicConfig(filename='/home/heisenberg/PycharmProjects/scrapy4/Scrapy/Scrapy/spiders/script.log', level=logging.DEBUG, format='%(asctime)s %(message)s')

# Example log messages
logging.info('Starting schedule2.py script')

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
    logging.info(f'Executing to_csv.py with {json_filename}')
    to_csv_script = '/home/heisenberg/PycharmProjects/scrapy4/Scrapy/Scrapy/spiders/to_csv.py'
    try:
        subprocess.run(['python', to_csv_script, json_filename], check=True)
        logging.info('to_csv.py executed successfully')
    except subprocess.CalledProcessError as e:
        logging.error(f'to_csv.py failed with error: {e}')

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
    """Schedule running spiders once and stop after completion."""
    d = run_product_spider()

    # Add callback to stop the reactor after spiders complete
    d.addBoth(lambda _: reactor.stop())

if __name__ == "__main__":
    # Configure logging
    configure_logging()

    # Start scheduling spiders
    schedule_spiders()

    # Start the reactor
    reactor.run()

#0 */2 * * * /home/heisenberg/PycharmProjects/scrapy4/Scrapy/Scrapy/spiders/run_schedule.sh >> /home/heisenberg/PycharmProjects/scrapy4/Scrapy/Scrapy/spiders/cron.log 2>&1
