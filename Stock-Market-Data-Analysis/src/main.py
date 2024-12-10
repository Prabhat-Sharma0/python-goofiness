from src.clean_data import clean_data
from src.load_data import alpha_api_key, load_data_from_alphavantage, load_data_from_twelve
from src.load_data import twelve_api_key
from src.plot_data import plot_stock_data
from src.transfom_data import calculate_moving_avg


def test_alpha_vantage_pipeline():
    print("Testing Alpha Vantage Batch Processing Pipeline...")
    data = load_data_from_alphavantage("AAPL", alpha_api_key)
    cleaned_data = clean_data(data)
    transformed_data = calculate_moving_avg(cleaned_data)
    # save_to_parquet(transformed_data, "alpha_batch_data.parquet")
    # load_and_validate_parquet("alpha_batch_data.parquet")
    print("Alpha Vantage Pipeline Test Completed.")
    plot_stock_data(transformed_data, "AAPL")


def test_twelve_data_pipeline():
    print("Testing Twelve Data Batch Processing Pipeline...")
    data = load_data_from_twelve("AAPL", twelve_api_key)
    cleaned_data = clean_data(data)
    transformed_data = calculate_moving_avg(cleaned_data)
    # save_to_parquet(transformed_data, "twelve_batch_data.parquet")
    # load_and_validate_parquet("twelve_batch_data.parquet")
    print("Twelve Data Pipeline Test Completed.")
    plot_stock_data(transformed_data, "AAPL")


if __name__ == "__main__":
    test_alpha_vantage_pipeline()
    # test_twelve_data_pipeline()
