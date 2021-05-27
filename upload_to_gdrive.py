import gspread
gc = gspread.service_account()
sh = gc.create('okex-aaa-SWAP')
sh.share(None, perm_type='anyone', role='reader')
worksheet = sh.get_worksheet(0)
worksheet.update('A1', 'time')
worksheet.update('B1', 'fundrate')

worksheet.update('A1:D1',[['fundrate','time','netprofit','price']])