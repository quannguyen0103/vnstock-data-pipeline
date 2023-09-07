BUCKET="vnstock/year_data"
FILE="/home/user/vnstock/src/data_processing/year_data.csv"

gsutil -o "GSUtil:parallel_composite_upload_threshold=150M" -m cp "$FILE" gs://"$BUCKET"
