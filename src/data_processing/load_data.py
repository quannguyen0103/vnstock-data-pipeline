from vnstock import *
import pandas as pd
import time
from datetime import datetime
import csv

sticker_list = list(listing_companies()["ticker"])

current_date = datetime.now().date()
vnstock_file = f"vnstock_{current_date}.csv"

stock_counter = 0
with open(vnstock_file, "w", newline="") as f:
    fieldnames = ['time', 'open', 'high', 'low', 'close', 'volume', 'ticker']
    writer = csv.DictWriter(f, fieldnames = fieldnames)
    writer.writeheader()
for sticker in sticker_list:
    try:
        stock_data = stock_historical_data(sticker, str(current_date), str(current_date), "1D")
        stock_data.to_csv(vnstock_file, mode = "a", header = False, index = False)
        print(f"Successfully get history data of {sticker} stock.")
        stock_counter += 1
        if stock_counter % 20 == 0:
            time.sleep(2)
        elif stock_counter % 30 == 0:
            time.sleep(2)
    except Exception as e:
        print(f"{e} happens at {sticker} stock. Skip")

print("All done.")
