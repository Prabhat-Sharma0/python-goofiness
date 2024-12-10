from pyspark.sql.functions import avg
from pyspark.sql.window import Window


def calculate_moving_avg(df, window_size=20): 
    print(f"Calculating {window_size}-period moving average...")
    window_spec = Window.orderBy("timestamp").rowsBetween(-window_size + 1, 0)
    df = df.withColumn("sma", avg("close").over(window_spec))
    print("Moving average calculation complete.")
    return df
