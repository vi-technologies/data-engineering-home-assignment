from awsglue.context import GlueContext
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import math

sc = SparkContext()
glueContext = GlueContext(sc)

spark = glueContext.spark_session

database_name = 'assignment_database_yuval_dror'
table_name = 'stocks_data'
output_bucket_uri = 's3://data-engineer-assignment-yuval-dror/data/output'

stocks_data_df = glueContext.create_data_frame.from_catalog(
    database=database_name,
    table_name=table_name
)

# Fill close price for missing dates - if there are any
stocks_data_df.withColumnRenamed(
    'Date', 'date'
)
ticker_date_window = Window.partitionBy('ticker').orderBy('date')
stocks_data_df = stocks_data_df.withColumn(
    'close_filled', 
    F.last('close', ignorenulls=True).over(ticker_date_window)
)

window_spec_lag = Window.partitionBy('ticker') \
    .orderBy('date') \
    .rowsBetween(Window.unboundedPreceding, -1)

stocks_data_df = stocks_data_df.withColumn(
    'prev_close', 
    F.last('close_filled', ignorenulls=True).over(window_spec_lag)
)
stocks_data_df = stocks_data_df \
    .withColumn('close', F.coalesce(F.col('close'), F.col('close_filled')))

# Calculate returns
stocks_data_df = stocks_data_df \
    .withColumn(
        'returns', 
        (F.col('close') - F.col('prev_close')) / F.col('prev_close') * 100
    )

# Calculate average daily returns
avg_daily_returns_df = stocks_data_df.groupby('date') \
    .agg(F.avg('returns').alias('average_return')).orderBy('date')
avg_daily_returns_df.write.format('parquet').save(f'{output_bucket_uri}/avg_daily_returns/')


# Calculate highest worth stocks
highest_worth_df = stocks_data_df.withColumn(
        'worth', F.col('close') * F.col('volume')
    ).groupby('ticker') \
    .agg(F.avg('worth').alias('value'))
highest_worth_df.write.format('parquet').save(f'{output_bucket_uri}/highest_worth/')


# Calculate most volatile stocks
annualized_volatility_df = stocks_data_df.groupby('ticker') \
    .agg(F.stddev('returns').alias('standard_deviation')) \
    .withColumn(
        'annualized_standard_deviation', 
        F.col('standard_deviation') * math.sqrt(252) # Assuming ~252 trading days in a year
    )
annualized_volatility_df.write.format('parquet').save(f'{output_bucket_uri}/annualized_volatility/')


# Calculate 30 days returns (assuming 30 trading days)
_30_days_returns_df = stocks_data_df.withColumn(
        'price_30_days_prior', F.lag('close_filled', 30).over(ticker_date_window)
    ).withColumn(
        'date_30_days_prior', F.lag('date', 30).over(ticker_date_window)
    ).filter(F.col('price_30_days_prior').isNotNull()) \
    .withColumn(
        '30_days_change', 
        (F.col('close') - F.col('price_30_days_prior')) \
        / F.col('price_30_days_prior') * 100
    ) \
    .withColumnRenamed(
        'date', 'start_date' 
    ).withColumn('date', F.concat(F.col('start_date'), F.lit(' - '), F.col('date_30_days_prior'))) \
    .sort('30_days_change', ascending=False) \
    .select('ticker','date', '30_days_change')
_30_days_returns_df.write.format('parquet').save(f'{output_bucket_uri}/30_days_returns/')
