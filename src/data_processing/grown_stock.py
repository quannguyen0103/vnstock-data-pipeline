from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd

spark = SparkSession.builder \
                    .appName("select_column") \
                    .getOrCreate()

year_file = "gs://vnstock/one_year_data/vnstock_1_year.csv"
daily_files = "gs://vnstock/daily_data/"

data = spark.read.csv([year_file, daily_files], header = True, inferSchema = True)

data.createOrReplaceTempView("stock")

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

query = """
WITH stock_data AS (
    SELECT
        ticker,
        close,
        date_format(time, 'yyyy-MM-dd') as date,
        LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date_format(time, 'yyyy-MM-dd')) AS prev_close,
        AVG(close) OVER (PARTITION BY ticker ORDER BY date_format(time, 'yyyy-MM-dd') ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS ma5
    FROM
        stock
),
filtered_stocks AS (
    SELECT
        ticker,
        close,
        date,
        prev_close,
        ma5,
        date_sub(date, 3 * 30) AS three_months_ago,
        DAYOFWEEK(date_sub(date, 3 * 30)) AS day_of_week_3_months_ago
    FROM
        stock_data
),
stocks_to_exclude AS (
    SELECT DISTINCT
        fs.ticker
    FROM
        filtered_stocks AS fs
    WHERE EXISTS (
        SELECT 1
        FROM
            filtered_stocks AS inner_fs
        WHERE
            inner_fs.ticker = fs.ticker
            AND date_trunc('month', inner_fs.date) = date_trunc('month', fs.three_months_ago)
            AND inner_fs.day_of_week_3_months_ago BETWEEN 2 AND 6
            AND (inner_fs.ma5 >= 1.05 * inner_fs.prev_close OR inner_fs.ma5 <= 0.95 * inner_fs.prev_close)
    )
)
SELECT
    fs.ticker,
    fs.close,
    fs.date,
    fs.ma5
FROM
    filtered_stocks AS fs
WHERE
    NOT EXISTS (
        SELECT 1
        FROM
            stocks_to_exclude AS se
        WHERE
            se.ticker = fs.ticker
    )
    AND ABS((fs.ma5 - fs.prev_close) / fs.prev_close) <= 0.05;
"""
result = spark.sql(query)

result.write \
    .option("header", "True") \
    .mode("overwrite") \
    .csv("gs://dataproc-1998/vnstock")

output = "gs://dataproc-1998/vnstock/*.csv"
df = spark.read.csv(output, header = True, inferSchema = True)
df_pandas = df.toPandas()
df_pandas.to_csv("gs://growth_stock/data/growth_stock.csv", index=False)

spark.stop()

