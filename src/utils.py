import pandas as pd


def load_and_prepare_data(csv_file_path):
    data = pd.read_csv(csv_file_path)
    data["Date"] = pd.to_datetime(data["Date"])
    return data.sort_values(by=["ticker", "Date"])


def save_to_csv(df, output_file_path):
    df.to_csv(output_file_path, index=False)
