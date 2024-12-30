import os
import pandas as pd


def load_and_prepare_data(csv_file_path):
    data = pd.read_csv(csv_file_path)
    data["Date"] = pd.to_datetime(data["Date"])
    return data.sort_values(by=["ticker", "Date"])


def calculate_average_daily_return(df):
    df["close"] = df["close"].fillna(method="ffill").fillna(method="bfill")
    df["daily_return"] = df.groupby("ticker")["close"].pct_change()
    df = df.dropna(subset=["daily_return"])
    result = df.groupby(["ticker", "Date"])["daily_return"].mean().reset_index()
    result.columns = ["ticker", "Date", "average_daily_return"]
    result["average_daily_return"] = result["average_daily_return"].round(5)
    return result


def save_to_csv(df, output_file_path):
    df.to_csv(output_file_path, index=False)


if __name__ == "__main__":
    csv_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "stocks_data.csv"
    )
    output_file_path = os.path.join(
        os.path.dirname(csv_file_path), "average_daily_return.csv"
    )

    data = load_and_prepare_data(csv_file_path)
    average_daily_return = calculate_average_daily_return(data)
    save_to_csv(average_daily_return, output_file_path)

    print(f"Average daily return saved to {output_file_path}")
