from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
import time
import os
import pandas as pd
import yfinance as yf
from typing import List
import requests



RUN_download_stocks = False
RUN_filter_stocks = True

def download_stocks_csv(download_dir='downloads/'):
    """
    Automates the process of downloading the CSV file of all stocks from the Nasdaq screener.

    Args:
        download_dir (str): Directory to save the downloaded CSV file.

    Returns:
        str: The path to the downloaded file.
    """
    # URL of the Nasdaq stock screener
    url = 'https://www.nasdaq.com/market-activity/stocks/screener'

    # Set up the Selenium WebDriver with custom download directory
    options = webdriver.ChromeOptions()
    prefs = {
        "download.default_directory": os.path.abspath(download_dir),
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    }
    options.add_experimental_option("prefs", prefs)

    # Create the download directory if it doesn't exist
    os.makedirs(download_dir, exist_ok=True)

    # Delete any existing CSV files in the download directory
    for file in os.listdir(download_dir):
        if file.endswith('.csv'):
            file_path = os.path.join(download_dir, file)
            os.remove(file_path)
            print(f"Deleted previous file: {file_path}")

    driver = webdriver.Chrome(options=options)

    try:
        # Open the URL
        driver.get(url)

        # Wait for the "Download CSV" button to be clickable
        download_button = WebDriverWait(driver, 10).until(
            ec.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Download CSV')]"))
        )

        # Click the "Download CSV" button
        download_button.click()

        # Wait for the download to complete (adjust the sleep time if needed)
        time.sleep(10)

        # Find the downloaded file in the directory
        files = os.listdir(download_dir)
        csv_files = [f for f in files if f.endswith('.csv')]
        if csv_files:
            downloaded_file = os.path.join(download_dir, csv_files[0])
            print(f"Downloaded file: {downloaded_file}")
            return downloaded_file
        else:
            raise FileNotFoundError("CSV file was not downloaded.")

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    finally:
        # Close the browser
        driver.quit()

def get_moving_avg(tickers: List[str], batch_size=5, sleep=2) -> pd.DataFrame:
    """
    Fetches 50, 100, 200-day moving averages and current price
    for a list of tickers using batched yfinance.download calls.

    Args:
        tickers (List[str]): List of ticker symbols.
        batch_size (int): Number of tickers per request batch.
        sleep (int): Seconds to wait between each batch (to avoid rate-limiting).

    Returns:
        pd.DataFrame: A DataFrame with Symbol, 50/100/200 MAs, and Current Price.
    """
    results = []

    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        print(f"Downloading batch {i // batch_size + 1} / {len(tickers) // batch_size + 1}: {batch}")

        try:
            data = yf.download(
                tickers=batch,
                period="220d",
                interval="1d",
                group_by="ticker",
                threads=False,
                progress=False
            )
        except Exception as e:
            print(f"Batch failed for {batch}: {e}")
            time.sleep(sleep)
            continue

        for symbol in batch:
            try:
                # Handle single ticker format
                symbol_data = data if len(batch) == 1 else data[symbol]
                symbol_data = symbol_data.dropna(subset=["Close"])

                if len(symbol_data) < 200:
                    continue

                symbol_data['50_MA'] = symbol_data['Close'].rolling(50).mean()
                symbol_data['100_MA'] = symbol_data['Close'].rolling(100).mean()
                symbol_data['200_MA'] = symbol_data['Close'].rolling(200).mean()

                latest = symbol_data.iloc[-1]

                results.append({
                    "Symbol": symbol,
                    "50_day_MA": latest["50_MA"],
                    "100_day_MA": latest["100_MA"],
                    "200_day_MA": latest["200_MA"],
                    "Current Price": latest["Close"]
                })

            except Exception as e:
                print(f"Failed to process {symbol}: {e}")

        time.sleep(sleep)  # Avoid rate limits

    return pd.DataFrame(results)

def read_and_filter_stocks(market_cap_threshold=2e9, last_sale_threshold=150):
    """
    Reads the CSV file in the fixed download directory, filters stocks by market cap,
    and calculates an indicator for "Last Sale".

    Args:
        market_cap_threshold (float): Filter out stocks with market cap lower than this value.
        last_sale_threshold (float): Threshold for calculating the "Last Sale" indicator.

    Returns:
        pd.DataFrame: DataFrame with "Symbol", "Market Cap", "Country", "Last Sale", and the indicator.
    """
    # Fixed download directory
    download_dir = 'downloads/'

    # Locate the most recent CSV file in the download directory
    files = os.listdir(download_dir)
    csv_files = [f for f in files if f.endswith('.csv')]

    if not csv_files:
        raise FileNotFoundError("No CSV file found in the specified directory.")

    # Assuming there's only one CSV file or taking the most recent one
    csv_file = os.path.join(download_dir, csv_files[0])

    # Read the CSV into a DataFrame
    df = pd.read_csv(csv_file)

    # Rename the columns for easier access (if necessary)
    df.columns = [col.strip() for col in df.columns]  # Remove any extra whitespace from column names

    # Filter stocks based on Market Cap threshold
    df['Market Cap'] = pd.to_numeric(df['Market Cap'], errors='coerce')  # Convert to numeric (handling errors)
    df = df[df['Market Cap'] >= market_cap_threshold]

    print(f"Market Cap filter threshold: {market_cap_threshold:,.0f} $$$")
    print(f"Remaining rows: {len(df)}")

    # Filter out stocks that contain "^" in the symbol
    df = df[~df['Symbol'].str.contains(r'\^')]

    # Replace "/" with "-" in the "Name" column (if present)
    df['Symbol'] = df['Symbol'].str.replace('/', '-', regex=False)

    # Create the "Last Sale" column and convert it to numeric
    df['Last Sale'] = df['Last Sale'].replace({r'\$': ''}, regex=True)  # Remove the "$" sign
    df['Last Sale'] = pd.to_numeric(df['Last Sale'], errors='coerce')  # Convert to numeric

    # Calculate the indicator for "Last Sale" being smaller than the threshold
    df['Affordable Indicator'] = df['Last Sale'].apply(lambda x: 1 if x < last_sale_threshold else 0)

    # Filter stocks based on Affordability
    df = df[df['Affordable Indicator'] == 1]

    print(f"Affordability threshold: {last_sale_threshold:,.0f} $$$")
    print(f"Remaining rows: {len(df)}")

    df_ma = get_moving_avg(df['Symbol'].tolist())
    df = pd.merge(df, df_ma, on='Symbol', how='left')

    print(df)

if RUN_download_stocks:
    download_stocks_csv()

if RUN_filter_stocks:
    #read_and_filter_stocks(market_cap_threshold=250e9, last_sale_threshold=150)
    ticker = 'AAPL'  # Use any known good ticker

    try:
        print("Requesting historical data for:", ticker)
        df = yf.download(ticker, period="5d", interval="1d", progress=True, threads=False)
        print(df)
        if df.empty:
            print("⚠️ No data returned.")
        else:
            print("✅ Data returned successfully.")
    except Exception as e:
        print(f"❌ Exception occurred: {type(e).__name__}: {e}")




