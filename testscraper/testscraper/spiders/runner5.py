import json, gspread

f = open('data2.json')
files = json.load(f)
final_data_average_momentum = []
final_data_buy_days_average = []
final_data_sell_days_average = []
final_data_probability = []
final_data_avg_profit = []
gc = gspread.service_account(filename='keys.json')
with open('config.json', 'r') as config_file:
    config = json.load(config_file)
global_index = gc.open_by_url(config['global_index'])
worksheet = global_index.worksheet('Supporting Data')
stock_list = worksheet.get('C4:C6000')
stock_list = [s[0] for s in stock_list if s]
for stock_data in files:
    try:
        row = stock_list.index(stock_data['stock']) + 4
    except:
        continue
    final_data_probability.append({'range': f'AD{row}', 'values': [[str(stock_data['probability'])]]})
    final_data_buy_days_average.append({'range': f'AE{row}', 'values': [[str(stock_data['buy_days_average'])]]})
    final_data_sell_days_average.append({'range': f'AF{row}', 'values': [[str(stock_data['sell_days_average'])]]})
    final_data_average_momentum.append({'range': f'AG{row}', 'values': [[str(stock_data['average_momentum'])]]})
    final_data_avg_profit.append({'range': f'AH{row}', 'values': [[str(stock_data['avg_profit'])]]})
worksheet.batch_clear(['AD4:AD6000', 'AE4:AE6000',  'AF4:AF6000',  'AG4:AG6000', 'AH4:AH6000'])
worksheet.batch_update(final_data_probability)
worksheet.batch_update(final_data_buy_days_average)
worksheet.batch_update(final_data_sell_days_average)
worksheet.batch_update(final_data_average_momentum)
worksheet.batch_update(final_data_avg_profit)
