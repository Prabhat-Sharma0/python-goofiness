import requests
import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Batch-Processing").getOrCreate()
alpha_api_key = "______"
twelve_api_key = "______"


def load_data_from_twelve(symbol, api_key, interval="1min"):
    print("Fetching Data from Twelve-Data API...")
    url = f"https://api.twelvedata.com/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "apikey": api_key,
        "outputsize": "5000"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if "values" in data:
        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df.rename(columns={"datetime": "timestamp"}, inplace=True)
        numeric_columns = ["open", "high", "low", "close", "volume"]
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")
        print("Data Fetched successfully.")
        return spark.createDataFrame(df)
    else: 
        print("Error in fetching in data: ", data)
        return spark.createDataFrame([])


def load_data_from_alphavantage(symbol, api_key, interval="1min"):
    print("Fetching Data from Alpha-Vantage API...")
    url = f"https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_INTRADAY",
        "symbol": symbol,
        "interval": interval,
        "apikey": api_key,
        "outputsize": "compact"
    }
    
    response = requests.get(url, params=params)
    data = response.json()
    
    if "Time Series (1min)" in data:
        time_series_key = next(iter(data.keys() - {"Meta Data"}))
        
        df = pd.DataFrame.from_dict(data[time_series_key], orient="index")
        df.reset_index(inplace=True)
        df.columns = ["timestamp", "open", "high", "low", "close", "volume"]
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        print("Data Fetched successfully.")
        return spark.createDataFrame(df)
    else: 
        print("Error in fetching in data: ", data)
        return spark.createDataFrame([])


def load_and_validate_parquet(file_path):
    print(f"Loading data from {file_path}...")
    df = spark.read.parquet(file_path)
    df.show(5)
    return df


# Validate Alpha Vantage Data
# alpha_validated_data = load_and_validate_parquet("alpha_batch_data.parquet")

# Validate Twelve Data
# twelve_validated_data = load_and_validate_parquet("twelve_batch_data.parquet")
