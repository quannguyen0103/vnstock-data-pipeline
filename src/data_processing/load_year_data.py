from vnstock import *
import pandas as pd
import time
import csv

sticker_list = list(listing_companies()["ticker"])

vnstock_file = "vnstock_1year.csv"
error_file = "error_1year.csv"
error_dict = {}
stock_counter = 0
with open(vnstock_file, "w", newline="") as f:
    fieldnames = ['time', 'open', 'high', 'low', 'close', 'volume', 'ticker']
    writer = csv.DictWriter(f, fieldnames = fieldnames)
    writer.writeheader()
for sticker in sticker_list:
    try:
        stock_data = stock_historical_data(sticker, "2022-08-11", "2023-08-11", "1D")
        stock_data.to_csv(vnstock_file, mode = "a", header = False, index = False)
        print(f"Successfully get history data of {sticker} stock.")
        stock_counter += 1
        if stock_counter % 20 == 0:
            time.sleep(2)
        elif stock_counter % 30 == 0:
            time.sleep(2)
    except Exception as e:
        print(f"{e} happens at {sticker} stock. Successfully skip & write {sticker} stock to error_dict.")
        error_dict[sticker] = str(e)

with open(error_file, "w", newline="") as f:
    csv_writer = csv.writer(f)
    csv_writer.writerow(["stock", "error"])
    for stock, error in error_dict.items():
        csv_writer.writerow([stock, error])
print(f"Successfully write error stocks to {error_file}.")
print("All done.")
