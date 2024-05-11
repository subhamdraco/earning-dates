from selenium.webdriver.chrome.options import Options
import pandas as pd
from datetime import datetime
import gspread
import json
import logging
import yfinance as yf
from tqdm import tqdm
from selenium import webdriver
from io import StringIO


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

    def get_earnings_data(self, stock):
        try:
            # driver.get(url)
            # driver.maximize_window()
            # page_source = StringIO(str(driver.page_source))
            # table = pd.read_html(page_source)[0]

            tick = yf.Ticker(stock)
            table = tick.get_earnings_dates()
            if isinstance(table, pd.DataFrame):
                table.reset_index(inplace=True)
            else:
                return None
            # table.columns = ['Symbol', 'Company', 'Earnings_Date', 'EPS_Estimate', 'Reported_EPS', 'Surprise']
            table.columns = ['Earnings_Date', 'EPS_Estimate', 'Reported_EPS', 'Surprise']
            # table['Earnings_Date'] = pd.to_datetime(table['Earnings_Date'].apply(lambda x: x[0:12]), format='mixed')
            table['Earnings_Date'] = pd.to_datetime(table['Earnings_Date'], format='mixed')

            df = table[table['Earnings_Date'] > self.today_date]
            df2 = table[table['Earnings_Date'] < self.today_date]
            if not df.empty:
                earning_date = df.tail(1)['Earnings_Date'].iloc[0].strftime('%d/%m/%Y')
                future_eps = df.tail(1)['EPS_Estimate'].iloc[0]
                future_reported = df.tail(1)['Reported_EPS'].iloc[0]
                table.dropna(inplace=True)
                non_na_tables = table.loc[(table['Earnings_Date'] != '-') & (table['EPS_Estimate'] != '-') & (
                            table['Reported_EPS'] != '-') & (table['Surprise'] != '-')]
                if not non_na_tables.empty:
                    earning_data = non_na_tables.head(1).iloc[0]
                    recent_date = df2.head(1)['Earnings_Date'].iloc[0].strftime('%d/%m/%Y')
                    earning_data = [[earning_date, str(future_eps).replace('nan','0'), str(earning_data['EPS_Estimate']).replace('nan','0'), str(future_reported).replace('nan','0'),
                                     str(earning_data['Reported_EPS']).replace('nan','0'), str(earning_data['Surprise']).replace('nan','0'), recent_date]]
                else:
                    earning_data = [[earning_date, '-', '-', '-', '-', '-', '-']]
                return earning_data
            else:
                raise ValueError('No tables found')

        except Exception as e:
            logging.info(f'{stock} : {str(e)}')

    def connect_to_gs(self, gs_name):
        return self.gc.open_by_url(gs_name)

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
            # url = self.config['url'].replace('$date', self.today_date).replace('$stock', stock)
            # earning_date = self.get_earnings_data(url)
            # return earning_date

    def get_stocks(self):
        global_index = self.connect_to_gs(self.config['global_index'])
        worksheet = global_index.worksheet('Supporting Data')
        worksheet.batch_clear(['AB4:AH6000'])
        stock_list = worksheet.get('C4:C6000')
        stock_list = [s[0] for s in stock_list]
        return stock_list

    def main(self):
        # try:
        #     options = Options()
        #     options.add_argument('--headless=new')
        #     options.add_argument('--disable-gpu')
        #     driver = webdriver.Chrome(options=options)
        # except Exception as e:
        #     print('Error: ' + str(e))
        #     logging.info(str(e))
        #     raise e
        stocks = self.get_stocks()
        global_index = self.connect_to_gs(self.config['global_index'])
        global_worksheet = global_index.worksheet('Supporting Data')
        for stock in tqdm(stocks, desc='Collecting Dates'):
            row = stocks.index(stock) + 4
            # url = self.config['url'].replace('$date', self.today_date).replace('$stock', stock)
            earning_data = self.get_earnings_data(stock)
            if earning_data:
                self.final_data.append({'range': f'AB{row}:AH{row}', 'values': earning_data})
                logging.info(f'{earning_data} for {stock} found')
                # print(f'{stock} : found')
            else:
                continue
            if len(self.final_data) == 10:
                global_worksheet.batch_update(self.final_data)
                self.final_data = []
       # driver.quit()
        if self.final_data:
            global_worksheet.batch_update(self.final_data)


if __name__ == "__main__":
    e = EarningData()
    e.main()
    # tick = yf.Ticker('TRTL')
    # table = tick.get_earnings_dates()
    # print(table)

    #print(e.get_earnings_data('TRTL'))
    # w = e.connect_to_gs('https://docs.google.com/spreadsheets/d/1U8uzF7g02NiOS1f-271TybGwT3EB7ICGr2fsF5RgsDU/edit#gid=1542175352')
    # worksheet = w.worksheet('Supporting Data')
    # worksheet.batch_clear(['AB4:AH6000'])
    # print(table.columns.to_list())
    # print(type(table.columns))
    # print(table)
    # print(e.get_earnings_data('USFD'))
