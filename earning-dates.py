from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import pandas as pd
from datetime import datetime
import gspread
import json
import logging
import yfinance as yf
from selenium.webdriver.common.by import By
from tqdm import tqdm
from retry import retry


class EarningData:

    def __init__(self):

        self.today_date = datetime.today().strftime('%Y-%m-%d')
        self.gc = gspread.service_account(filename='keys.json')
        with open('config.json', 'r') as config_file:
            self.config = json.load(config_file)
        self.final_data = []
        logging.basicConfig(
            level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            format="%(asctime)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            filename="app.log",  # Log messages will be written to this file
            filemode="w"  # 'w' for overwrite, 'a' for append
        )

    def get_earnings_data(self, url):
        options = Options()
        options.add_argument('--headless=new')
        options.add_argument('--user-agent=Chrome/120.0.6099.216 (IST)')
        driver = webdriver.Chrome(options=options)
        try:
            driver.get(url)
            driver.maximize_window()
            driver.implicitly_wait(10)
            page_source = driver.page_source
            table = pd.read_html(str(page_source))[0]
            table['Earnings Date'] = pd.to_datetime(table['Earnings Date'].apply(lambda x: x[0:12]), format='mixed')
            df = table[table['Earnings Date'] > self.today_date]
            if not df.empty:
                earning_date = df.tail(1)['Earnings Date'].iloc[0].strftime('%d/%m/%Y')
                driver.quit()
                return earning_date
            else:
                driver.quit()
                raise ValueError('No tables found')

        except ValueError as e:
            driver.quit()
            logging.info(f'{url[-4:]} : {str(e)}')

    def connect_to_gs(self, gs_name):
        return self.gc.open_by_url(gs_name)

    @retry(tries=3)
    def get_earnings_date_alternative(self, stock):
        try:
            tick = yf.Ticker(stock)
            table = tick.get_earnings_dates()
            if not table.empty:
                table.reset_index(inplace=True)
                table['Earnings Date'] = pd.to_datetime(table['Earnings Date'], format='mixed')
                df = table[table['Earnings Date'] > self.today_date]
                if not df.empty:
                    earning_date = df.tail(1)['Earnings Date'].iloc[0].strftime('%d/%m/%Y')
                    return earning_date
            else:
                raise ValueError('No tables found')
        except Exception as e:
            logging.info(f'Error in {stock}: {str(e)}')
            # if str(e) == 'Earnings Date':
            url = self.config['url'].replace('$date', self.today_date).replace('$stock', stock)
            earning_date = self.get_earnings_data(url)
            return earning_date

    def get_stocks(self):
        global_index = self.connect_to_gs(self.config['global_index'])
        worksheet = global_index.worksheet('Supporting Data')
        stock_list = worksheet.get('C4:C5000')
        stock_list = [s[0] for s in stock_list]
        return stock_list

    def main(self):
        stocks = self.get_stocks()
        global_index = self.connect_to_gs(self.config['global_index'])
        global_worksheet = global_index.worksheet('Supporting Data')
        for stock in tqdm(stocks, desc='Collecting Dates'):
            row = stocks.index(stock) + 4
            # url = self.config['url'].replace('$date', self.today_date).replace('$stock', stock)
            earning_date = self.get_earnings_date_alternative(stock)
            if earning_date:
                self.final_data.append({'range': f'AB{row}', 'values': [[earning_date.replace("'", '')]]})
                logging.info(f'{earning_date} for {stock} found')
                # print(f'{stock} : found')

            if len(self.final_data) == 10:
                global_worksheet.batch_update(self.final_data)
                self.final_data = []
        if self.final_data:
            global_worksheet.batch_update(self.final_data)


if __name__ == "__main__":
    e = EarningData()
    e.main()
    #print(e.get_earnings_date_alternative('ATI'))
