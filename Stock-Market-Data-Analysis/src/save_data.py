def save_to_parquet(df, file_path):
    print(f"Saving data to {file_path}...")
    df.write.mode("overwrite").parquet(file_path)
    print("Data saved successfully.")
