from scrapy import signals
from scrapy.crawler import CrawlerProcess
from scrapy.signalmanager import dispatcher
from stock_urls import InvestScrape  # Import your spider class

# Create a CrawlerProcess
process = CrawlerProcess(settings={
    'FEED_FORMAT': 'json',  # Output format
    'FEED_URI': 'data3.json'  # Output file
})


# Connect a callback function to the spider_closed signal
def spider_closed(spider):
    process.stop()


dispatcher.connect(spider_closed, signal=signals.spider_closed)

# Add your spider to the process
process.crawl(InvestScrape)

# Start the process
process.start()

