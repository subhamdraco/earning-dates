import os

import scrapy
import gspread, json


class InvestScrape(scrapy.Spider):
    name = 'Invest'
    gc = gspread.service_account(filename='keys.json')
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    global_index = gc.open_by_url(config['global_index'])
    worksheet = global_index.worksheet('Supporting Data')
    stock_list = worksheet.get('C4:C6000')
    stock_list = [
        s[0].replace('.SZ', '').replace('.SS', '').replace('.SI', '').replace('.T', '').replace('.PA', '').replace(
            '.BR', '').replace('.JK', '').replace('.F', '') for s in stock_list]
    start_urls = ['https://www.investing.com/search/?q=' + stock for stock in stock_list]

    custom_settings = {
        'FEED_FORMAT': 'json',
        'FEED_URI': 'urls.json',
        # 'DOWNLOAD_DELAY': 2
    }

    #
    def __init__(self, **kwargs):
        # Path to your JSON file
        file_path = "urls.json"
        super().__init__(**kwargs)
        if os.path.exists(file_path):
            # Open the file in write mode ('w')
            with open(file_path, 'w') as json_file:
                # Writing nothing effectively clears the file
                pass

    def parse(self, response):
        name = response.css('a.js-inner-all-results-quote-item')
        try:
            stock_name = response.css('span.second').getall()[1].split('>')[1].replace('</span', '')
        except:
            stock_name = None
        yield {
            'url': name.xpath('@href').get(),
            'stock': stock_name
        }
