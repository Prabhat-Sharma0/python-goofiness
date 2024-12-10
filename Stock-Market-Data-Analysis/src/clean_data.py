from pyspark.sql.functions import col


def clean_data(df): 
    print("Cleaning Data...")
    df = df.dropna()
    
    numeric_columns = ["open", "high", "low", "close", "volume"]
    for col_name in numeric_columns: 
        df = df.withColumn(col_name, col(col_name).cast("float"))
        
    print("Data Cleaning Complete...")
    return df
