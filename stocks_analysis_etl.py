import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, avg, stddev, expr
from pyspark.sql.window import Window
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext


# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create Glue context
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Input and output S3 paths
s3_path_data = "s3://data-engineer-assignment-dimatru/input/data/stocks_data.csv"
s3_path_output = "s3://data-engineer-assignment-dimatru/output/"


# Function to load the CSV file
def load_data(glueContext, path):
    try:
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [path]},
            format="csv",
            format_options={"withHeader": True}
        )
        df = dynamic_frame.toDF()
        return df.withColumn("date", col("date").cast("date"))
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise


# Function to calculate average daily return
def calculate_avg_daily_return(df):
    window_spec = Window.partitionBy("ticker").orderBy("date")
    df_with_return = df.withColumn("prev_close", lag("close", 1).over(window_spec)) \
        .withColumn("daily_return", (col("close") - col("prev_close")) / col("prev_close")) \
        .filter(col("prev_close").isNotNull())

    avg_daily_return = df_with_return.groupBy("date") \
        .agg(avg("daily_return").alias("average_return")) \
        .orderBy("date")

    return avg_daily_return


# Function to calculate the stock with the highest worth
def calculate_highest_worth_stock(df):
    df_with_worth = df.withColumn("worth", col("close") * col("volume"))
    highest_worth = df_with_worth.groupBy("ticker") \
        .agg(avg("worth").alias("value")) \
        .orderBy(col("value").desc()) \
        .limit(1)
    return highest_worth


# Function to calculate stock volatility
def calculate_stock_volatility(df):
    window_spec = Window.partitionBy("ticker").orderBy("date")
    df_with_return = df.withColumn("prev_close", lag("close", 1).over(window_spec)) \
        .withColumn("daily_return", (col("close") - col("prev_close")) / col("prev_close")) \
        .filter(col("prev_close").isNotNull())

    volatility = df_with_return.groupBy("ticker") \
        .agg(stddev("daily_return").alias("standard_deviation")) \
        .withColumn("standard_deviation", col("standard_deviation") * expr("sqrt(252)")) \
        .orderBy(col("standard_deviation").desc()) \
        .limit(1)
    return volatility


# Function to find top 3 30-day return dates
def calculate_top_3_30_day_returns(df):
    window_spec = Window.partitionBy("ticker").orderBy("date")
    df_with_30_day_return = df.withColumn("prev_30_day_close", lag("close", 30).over(window_spec)) \
        .withColumn("30_day_return", (col("close") - col("prev_30_day_close")) / col("prev_30_day_close"))

    top_3_returns = df_with_30_day_return.orderBy(col("30_day_return").desc()) \
        .select("ticker", "date") \
        .limit(3)
    return top_3_returns


# Main process
try:
    logger.info("Loading data...")
    df = load_data(glueContext, s3_path_data)

    logger.info("Calculating metrics...")
    avg_daily_return = calculate_avg_daily_return(df)
    stock_highest_worth = calculate_highest_worth_stock(df)
    stock_volatility = calculate_stock_volatility(df)
    top_3_30_day_return = calculate_top_3_30_day_returns(df)

    logger.info("Converting DataFrames to DynamicFrames...")
    avg_daily_return_dynamic = DynamicFrame.fromDF(avg_daily_return, glueContext, "avg_daily_return_dynamic")
    stock_highest_worth_dynamic = DynamicFrame.fromDF(stock_highest_worth, glueContext, "stock_highest_worth_dynamic")
    stock_volatility_dynamic = DynamicFrame.fromDF(stock_volatility, glueContext, "stock_volatility_dynamic")
    top_3_30_day_return_dynamic = DynamicFrame.fromDF(top_3_30_day_return, glueContext, "top_3_30_day_return_dynamic")

    logger.info("Saving results to S3...")
    glueContext.write_dynamic_frame.from_options(
        frame=avg_daily_return_dynamic,
        connection_type="s3",
        connection_options={"path": s3_path_output + "avg_daily_return"},
        format="parquet"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=stock_highest_worth_dynamic,
        connection_type="s3",
        connection_options={"path": s3_path_output + "stock_highest_worth"},
        format="parquet"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=stock_volatility_dynamic,
        connection_type="s3",
        connection_options={"path": s3_path_output + "stock_volatility"},
        format="parquet"
    )

    glueContext.write_dynamic_frame.from_options(
        frame=top_3_30_day_return_dynamic,
        connection_type="s3",
        connection_options={"path": s3_path_output + "top_3_30_day_return"},
        format="parquet"
    )


    logger.info("Processing completed successfully.")

except Exception as e:
    print(f"Error during processing: {str(e)}")


