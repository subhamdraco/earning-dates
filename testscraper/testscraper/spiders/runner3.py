import json, gspread

f = open('data.json')
files = json.load(f)
final_data_future = []
final_data_recent = []
gc = gspread.service_account(filename='keys.json')
with open('config.json', 'r') as config_file:
    config = json.load(config_file)
global_index = gc.open_by_url(config['global_index'])
worksheet = global_index.worksheet('Supporting Data')
stock_list = worksheet.get('C4:C6000')
stock_list = [s[0].replace('.SZ', '').replace('.SS', '').replace('.SI', '').replace('.T', '').replace('.PA', '').replace(
            '.BR', '').replace('.JK', '') for s in stock_list]
for stock_data in files:
    try:
        row = stock_list.index(stock_data['stock']) + 4
    except:
        continue
    final_data_future.append({'range': f'AB{row}', 'values': [[stock_data['future_date']]]})
    final_data_recent.append({'range': f'AC{row}', 'values': [[stock_data['recent_date']]]})

worksheet.batch_clear(['AB4:AB6000', 'AC4:AC6000'])
worksheet.batch_update(final_data_future)
worksheet.batch_update(final_data_recent)
