import pandas as pd
import scrapy
import json, os
from datetime import datetime


class DateScrape(scrapy.Spider):
    name = 'DateScrape'
    start_urls = []
    f = open('urls.json')
    files = json.load(f)
    for entry in files:
        if entry['url']:
            start_urls.append('https://www.investing.com' + entry['url'] + '-earnings')
    # start_urls = ['https://www.investing.com/equities/actuant-corp-earnings']
    custom_settings = {
        'FEED_FORMAT': 'json',
        'FEED_URI': 'data.json',
        'DOWNLOAD_DELAY': 5
    }

    def __init__(self, **kwargs):
        # Path to your JSON file
        file_path = "data.json"
        super().__init__(**kwargs)
        if os.path.exists(file_path):
            # Open the file in write mode ('w')
            with open(file_path, 'w') as json_file:
                # Writing nothing effectively clears the file
                pass

    def parse(self, response):
        earning_data = []
        res = response.xpath('//table[@class="genTbl openTbl ecoCalTbl earnings earningsPageTbl"]//tr')
        for row in res:
            earning_data.append(row.xpath("td//text()").getall())
        if earning_data:
            dates_ = pd.DataFrame({'dates': sorted(
                list(filter(None,
                            [datetime.strptime(date[0], "%b %d, %Y") if date else None for date in earning_data])))})
            dates_['dates'] = pd.to_datetime(dates_['dates'])
            today_date = datetime.today().strftime('%Y-%m-%d')
            future_dates = dates_[dates_['dates'] > today_date]
            recent_dates = dates_[dates_['dates'] < today_date]
            if not future_dates.empty and not recent_dates.empty:
                future_date = future_dates.tail(1)['dates'].iloc[0].strftime('%d/%m/%Y')
                recent_date = recent_dates.tail(1)['dates'].iloc[0].strftime('%d/%m/%Y')
            else:
                future_date = '-'
                recent_date = '-'
            stock = response.css("div.instrumentHead").xpath(
                "//h1[@class='float_lang_base_1 relativeAttr']//text()").get().split('(')[1].split(')')[0]
            yield {
                'future_date': future_date,
                'recent_date': recent_date,
                'stock': stock

            }
