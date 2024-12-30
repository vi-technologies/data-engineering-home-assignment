from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

spark = (
    SparkSession.builder.appName("rw_vilabs")
    .config("spark.driver.bindAddress", "127.0.0.1")  # Bind explicitly to localhost
    .getOrCreate()
)


def load_and_prepare_data_spark(file_path):
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    df = df.withColumn("Date", F.to_date("Date"))
    window_spec = Window.partitionBy("ticker").orderBy("Date")
    df = df.withColumn("close", F.last("close", ignorenulls=True).over(window_spec))
    df = df.withColumn(
        "close",
        F.when(
            F.col("close").isNull(),
            F.first("close", ignorenulls=True).over(window_spec),
        ).otherwise(F.col("close")),
    )

    return df


def calc_question_1_spark(df):
    window_spec = Window.partitionBy("ticker").orderBy("Date")
    df = df.withColumn(
        "daily_return",
        (F.col("close") - F.lag("close").over(window_spec))
        / F.lag("close").over(window_spec),
    )

    df = df.filter(F.col("daily_return").isNotNull())

    avg_daily_return = (
        df.groupBy("ticker", "Date")
        .agg(F.mean("daily_return").alias("average_daily_return"))
        .withColumn("average_daily_return", F.round("average_daily_return", 5))
    )

    result = avg_daily_return.groupBy("Date").agg(
        F.collect_list(F.struct("ticker", "average_daily_return")).alias(
            "average_return"
        )
    )

    return result


if __name__ == "__main__":
    data_path = "/home/ran/vi-labs/data-engineering-home-assignment/stocks_data.csv"
    data = load_and_prepare_data_spark(data_path)
    average_daily_return = calc_question_1_spark(data)
    average_daily_return.show(truncate=False)
