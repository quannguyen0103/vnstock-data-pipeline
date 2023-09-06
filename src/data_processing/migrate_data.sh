BUCKET="vnstock/daily_data"
FILE="/home/quangcloud123/vnstock/src/data_processing/vnstock_*.csv"

gsutil -o "GSUtil:parallel_composite_upload_threshold=150M" -m cp "$FILE" gs://"$BUCKET"
