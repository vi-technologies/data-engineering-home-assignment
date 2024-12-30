import os
from utils import save_to_s3, load_and_prepare_data_pandas, ensure_s3_bucket_exists
from tasks_pandas import (
    calc_question_1,
    calc_question_2,
    calc_question_3,
    calc_question_4,
)
from dotenv import load_dotenv


if __name__ == "__main__":
    load_dotenv()
    csv_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "stocks_data.csv"
    )
    bucket_name = "data-engineer-assignment-ranw"
    ensure_s3_bucket_exists(bucket_name)

    data = load_and_prepare_data_pandas(csv_file_path)

    average_daily_return = calc_question_1(data)
    save_to_s3(average_daily_return, bucket_name, "output/question_1_results.csv")

    highest_worth_stock = calc_question_2(data)
    save_to_s3(highest_worth_stock, bucket_name, "output/question_2_results.csv")

    most_volatile_stock = calc_question_3(data)
    save_to_s3(most_volatile_stock, bucket_name, "output/question_3_results.csv")

    top_3_30_day_returns = calc_question_4(data)
    save_to_s3(top_3_30_day_returns, bucket_name, "output/question_4_results.csv")
