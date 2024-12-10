import matplotlib.pyplot as plt


def plot_stock_data(df, symbol="AAPL"):
    df.show(5)  # Display the first 5 rows of the DataFrame
    df.printSchema()  # Check the schema for data type issues

    df = df.toPandas()

    plt.figure(figsize=(13, 9))
    plt.plot(df['timestamp'], df['close'], label='Closing Price', color='blue', alpha=0.7)
    plt.plot(df['timestamp'], df['sma_5'], label='20-period Moving Average', color='orange', alpha=0.7)

    plt.title(f'{symbol} Stock Price and 5-period Moving Average')
    plt.xlabel('Timestamp')
    plt.ylabel('Price ($)')
    plt.legend(loc='best')
    plt.grid(True)

    # Rotate x-axis labels to avoid overlap
    plt.xticks(rotation=45)

    # Display the plot
    plt.tight_layout()
    plt.show()
