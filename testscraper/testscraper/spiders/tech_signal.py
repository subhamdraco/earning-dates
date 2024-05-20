import pandas as pd
import scrapy, gspread
import json, os
from datetime import datetime, timedelta
import yfinance as yf


class EarningNews(scrapy.Spider):
    name = 'EarningNews'
    start_urls = []
    f = open('urls.json')
    files = json.load(f)
    gc = gspread.service_account(filename='keys.json')
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    global_index = gc.open_by_url(config['global_index'])
    worksheet = global_index.worksheet('Supporting Data')
    stock_list = worksheet.get('C4:C6000')
    stocks = [s[0] for s in stock_list]
    for entry in files:
        if entry['url']:
            if "?" in entry['url']:
                url = entry['url'].split("?")
                url = 'https://www.investing.com' + url[0] + '-earnings?' + url[1]
                start_urls.append(url)
            else:
                start_urls.append('https://www.investing.com' + entry['url'] + '-earnings')
    # start_urls = ['https://www.investing.com/equities/nanometrics-incor-earnings']

    custom_settings = {
        'FEED_FORMAT': 'json',
        'FEED_URI': 'data2.json',
        'DOWNLOAD_DELAY': 1.5
    }
    # def start_requests(self):
    #     for url in self.start_urls:
    #         yield scrapy.Request(url, meta={'playwright': True})

    def __init__(self, **kwargs):
        # Path to your JSON file
        file_path = "data2.json"
        super().__init__(**kwargs)
        if os.path.exists(file_path):
            # Open the file in write mode ('w')
            with open(file_path, 'w') as json_file:
                # Writing nothing effectively clears the file
                pass

    def create_date_format(self, date_string):
        date_object = datetime.strptime(date_string, "%b %d, %Y")
        formatted_date = date_object.date().strftime('%Y-%m-%d')
        return formatted_date

    def get_historical(self, stock):
        try:
            tick = yf.Ticker(stock)
            history = tick.history(period='max')
            history.reset_index(inplace=True)
            history['Return'] = history.Close.pct_change(1).apply(lambda x: x * 100)
            history.dropna(inplace=True)
            history['Avg_5_day_return'] = history.Return.rolling(5).mean()
            history = history[::-1]
            history.drop(['Dividends', 'Stock Splits', 'Open', 'High', 'Low', 'Volume'], axis=1, inplace=True)
            history['Date'] = pd.to_datetime(history['Date'], format='mixed')
        except:
            history = None
        return history

    def get_stock(self, stock):
        for s in self.stocks:
            if '.' in stock:
                return stock

            if stock in s:
                return s

    def parse(self, response):
        try:
            earning_data = []
            res = response.xpath('//table[@class="genTbl openTbl ecoCalTbl earnings earningsPageTbl"]//tr')
            for row in res:
                earning_data.append(row.xpath("td//text()").getall())
            earning_data = [self.create_date_format(s[0]) for s in earning_data if s]
            today_ = datetime.today().strftime('%Y-%m-%d')
            earning_data = sorted([d for d in earning_data if d < today_])
            earning_data.reverse()
            stock = \
                response.css("div.instrumentHead").xpath(
                    "//h1[@class='float_lang_base_1 relativeAttr']//text()").get().split(
                    '(')[1].split(')')[0]
            stock = self.get_stock(stock)
            history = self.get_historical(stock)
            buy_dates = []
            sell_dates = []
            highest_sell_prices = []
            sell_days_after_ = []
            lowest_buy_prices = []
            buy_days_before_ = []
            three_day_date_buy_ = []
            three_day_date_sell_ = []
            probability = []
            profits_ = []
            buy_days_average = 0
            sell_days_average = 0
            average_momentum = 0
            probability_ = 0
            avg_profit = 0
            if not history.empty or history is not None:
                if earning_data:
                    for i in range(len(earning_data)):
                        # sell date
                        if i == 0:
                            filtered_df = history[(history['Date'] >= earning_data[i]) & (history['Date'] <= today_)]
                            lowest_return_date = filtered_df.loc[filtered_df['Avg_5_day_return'].idxmin()]
                            sell_date = lowest_return_date.iloc[0] - timedelta(days=5)
                            three_day_date_sell = history[(history['Date'] == earning_data[i])].index[0]
                            num_rows, num_columns = history.shape
                            three_day_date_sell = history.iloc[(num_rows - three_day_date_sell) - 3].iloc[1]
                            three_day_date_sell_.append(three_day_date_sell)
                            highest_sell_price = lowest_return_date.iloc[1]
                            sell_days_after = datetime.strptime(sell_date.strftime('%Y-%m-%d'),
                                                                '%Y-%m-%d') - datetime.strptime(earning_data[i], '%Y-%m-%d')
                            sell_dates.append(sell_date.strftime('%Y-%m-%d'))
                            highest_sell_prices.append(highest_sell_price)
                            sell_days_after_.append(sell_days_after.days)
                        else:
                            filtered_df = history[
                                (history['Date'] >= earning_data[i]) & (history['Date'] <= earning_data[i - 1])]
                            lowest_return_date = filtered_df.loc[filtered_df['Avg_5_day_return'].idxmin()]
                            sell_date = lowest_return_date.iloc[0] - timedelta(days=5)
                            three_day_date_sell = history[(history['Date'] == earning_data[i])].index[0]
                            num_rows, num_columns = history.shape
                            three_day_date_sell = history.iloc[(num_rows - three_day_date_sell) - 3].iloc[1]
                            three_day_date_sell_.append(three_day_date_sell)
                            highest_sell_price = lowest_return_date.iloc[1]
                            sell_days_after = datetime.strptime(sell_date.strftime('%Y-%m-%d'),
                                                                '%Y-%m-%d') - datetime.strptime(earning_data[i], '%Y-%m-%d')
                            sell_dates.append(sell_date.strftime('%Y-%m-%d'))
                            highest_sell_prices.append(highest_sell_price)
                            sell_days_after_.append(sell_days_after.days)
                        # buy date
                        if i == len(earning_data) - 1:
                            date_ = datetime.strptime(earning_data[i], '%Y-%m-%d') - timedelta(days=30)
                            filtered_df = history[
                                (history['Date'] <= earning_data[i]) & (history['Date'] >= date_.strftime('%Y-%m-%d'))]
                            lowest_return_date = filtered_df.loc[filtered_df['Avg_5_day_return'].idxmin()]
                            buy_date = lowest_return_date.iloc[0] + timedelta(days=5)
                            three_day_date_buy = history[(history['Date'] == earning_data[i])].index[0]
                            num_rows, num_columns = history.shape
                            three_day_price_buy = history.iloc[(num_rows - three_day_date_buy) + 3].iloc[1]
                            three_day_date_buy_.append(three_day_price_buy)
                            lowest_buy_price = lowest_return_date.iloc[1]
                            buy_days_before = datetime.strptime(earning_data[i], '%Y-%m-%d') - datetime.strptime(
                                buy_date.strftime('%Y-%m-%d'), '%Y-%m-%d')
                            buy_dates.append(buy_date.strftime('%Y-%m-%d'))
                            lowest_buy_prices.append(lowest_buy_price)
                            buy_days_before_.append(buy_days_before.days)
                        else:
                            filtered_df = history[
                                (history['Date'] <= earning_data[i]) & (history['Date'] >= earning_data[i + 1])]
                            lowest_return_date = filtered_df.loc[filtered_df['Avg_5_day_return'].idxmin()]
                            buy_date = lowest_return_date.iloc[0] + timedelta(days=5)
                            lowest_buy_price = lowest_return_date.iloc[1]
                            buy_days_before = datetime.strptime(earning_data[i], '%Y-%m-%d') - datetime.strptime(
                                buy_date.strftime('%Y-%m-%d'), '%Y-%m-%d')
                            buy_dates.append(buy_date.strftime('%Y-%m-%d'))
                            three_day_date_buy = history[(history['Date'] == earning_data[i])].index[0]
                            num_rows, num_columns = history.shape
                            three_day_price_buy = history.iloc[(num_rows - three_day_date_buy) + 3].iloc[1]
                            three_day_date_buy_.append(three_day_price_buy)
                            lowest_buy_prices.append(lowest_buy_price)
                            buy_days_before_.append(buy_days_before.days)
                        if three_day_price_buy < three_day_date_sell:
                            probability.append(1)
                        else:
                            probability.append(0)
                        profit = highest_sell_price - lowest_buy_price
                        profits_.append(profit)

                    buy_days_average = sum(buy_days_before_) / len(buy_days_before_)
                    sell_days_average = sum(sell_days_after_) / len(sell_days_after_)
                    average_momentum = buy_days_average + sell_days_average
                    probability_ = sum(probability) / len(probability) * 100
                    avg_profit = sum(profits_) / len(profits_)

            yield {
                'stock': stock,
                'buy_days_average': -int(buy_days_average),
                'sell_days_average': int(sell_days_average),
                'average_momentum': int(average_momentum),
                'probability': probability_,
                'avg_profit': avg_profit
            }
        except:
            yield {
                'stock': 'stock',
                'buy_days_average': 0,
                'sell_days_average': 0,
                'average_momentum': 0,
                'probability': 0,
                'avg_profit' : 0
            }