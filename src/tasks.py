import os
from utils import load_and_prepare_data, save_to_csv


def calc_question_1(df):
    df["close"] = df["close"].fillna(method="ffill").fillna(method="bfill")
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()
    df = df.dropna(subset=["daily_return"])
    result = df.groupby(["ticker", "Date"])["daily_return"].mean().reset_index()
    result.columns = ["ticker", "Date", "average_daily_return"]
    result["average_daily_return"] = result["average_daily_return"].round(5)
    grouped = (
        result.groupby("Date")
        .apply(lambda x: dict(zip(x["ticker"], x["average_daily_return"])))
        .reset_index(name="average_return")
    )
    return grouped


def calc_question_2(df):
    df["daily_worth"] = df["close"] / df["volume"]
    average_daily_worth = df.groupby("ticker")["daily_worth"].mean().reset_index()
    average_daily_worth.columns = ["ticker", "value"]
    highest_worth = average_daily_worth.loc[average_daily_worth["value"].idxmax()]
    return highest_worth.to_frame().transpose()


def calc_question_3(df):
    trading_days = 252
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()
    df = df.dropna(subset=["daily_return"])
    volatility = (
        df.groupby("ticker")["daily_return"]
        .std()
        .reset_index()
        .rename(columns={"daily_return": "std_dev"})
    )
    volatility["standard_deviation"] = volatility["std_dev"] * (trading_days**0.5)
    most_volatile = volatility.loc[
        volatility["standard_deviation"].idxmax(), ["ticker", "standard_deviation"]
    ]
    return most_volatile.to_frame().transpose()


if __name__ == "__main__":
    csv_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "stocks_data.csv"
    )

    # create dir for results
    os.makedirs(os.path.join(os.path.dirname(csv_file_path), "output"), exist_ok=True)

    # question 1
    output_file_path = os.path.join(
        os.path.dirname(csv_file_path), "output", "average_daily_return.csv"
    )

    data = load_and_prepare_data(csv_file_path)
    average_daily_return = calc_question_1(data)
    save_to_csv(average_daily_return, output_file_path)

    # question 2
    output_file_path = os.path.join(
        os.path.dirname(csv_file_path), "output", "highest_average_daily_worth.csv"
    )
    data = load_and_prepare_data(csv_file_path)
    highest_worth_stock = calc_question_2(data)
    save_to_csv(highest_worth_stock, output_file_path)

    # question 3
    output_file_path = os.path.join(
        os.path.dirname(csv_file_path), "output", "most_volatile_stock.csv"
    )
    data = load_and_prepare_data(csv_file_path)
    most_volatile_stock = calc_question_3(data)
    save_to_csv(most_volatile_stock, output_file_path)
