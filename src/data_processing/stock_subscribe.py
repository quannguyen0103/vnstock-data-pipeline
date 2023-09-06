from google.cloud import pubsub_v1
from vnstock import *
import pandas as pd
import requests
import os

key_path = r"/home/quangcloud123/high-task-393315-617ea482a1a2.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path("high-task-393315", "vnstock")

TOKEN = "6692894008:AAF6LBoZ785jhn6614PGtbia1eo1XiGITP4"
chat_id = "-933721799"

df = price_board("SSI,VND,HPG,NKG,VIC,NHA,CEO,LDG,VIX")
price = {"SSI": 40000, "VND": 29000, "HPG": 27000, "NKG": 19000, "VIC": 80000, "NHA": 30000, "CEO": 25000, "LDG": 6000, "VIX": 20000}
current_time = datetime.now() + timedelta(hours=7)
df["time"] = current_time.strftime('%Y-%m-%d %H:%M:%S')
data = df.loc[:, ["Mã CP", "Giá", "time"]]
data = data.rename(columns={"Mã CP": "ticker", "Giá": "price"})
for _, row in data.iterrows():
	ticker = row["ticker"]
	df_price = row["price"]
	dict_price = price[ticker]
	percentage_drop = ((dict_price - df_price) / dict_price) * 100
	if percentage_drop >= 10:  # Checking if the drop is 10% or more
		message =  f"Trời má, {ticker} giảm {percentage_drop:.2f}% rồi kìa!!!"
		url = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={message}"
		print(requests.get(url).json())
	json_data = row.to_json()
	future = publisher.publish(topic_path, data=json_data.encode("utf-8"))
	print(f"Published message: {future.result()}")

print("DataFrame published to Pub/Sub as JSON messages.")
