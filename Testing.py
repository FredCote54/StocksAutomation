from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
import time
import os
import pandas as pd
import yfinance as yf

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
#Fred
#download_stocks_csv()

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

    # Check the first few rows to understand the structure
    print(df.head())

    # Rename the columns for easier access (if necessary)
    df.columns = [col.strip() for col in df.columns]  # Remove any extra whitespace from column names

    # Filter stocks based on Market Cap threshold
    df['Market Cap'] = pd.to_numeric(df['Market Cap'], errors='coerce')  # Convert to numeric (handling errors)
    df = df[df['Market Cap'] >= market_cap_threshold]

    # Filter out stocks that contain "^" in the symbol
    df = df[~df['Symbol'].str.contains(r'\^')]

    # Replace "/" with "-" in the "Name" column (if present)
    df['Symbol'] = df['Symbol'].str.replace('/', '-', regex=False)

    # Create the "Last Sale" column and convert it to numeric
    df['Last Sale'] = df['Last Sale'].replace({r'\$': ''}, regex=True)  # Remove the "$" sign
    df['Last Sale'] = pd.to_numeric(df['Last Sale'], errors='coerce')  # Convert to numeric

    # Calculate the indicator for "Last Sale" being smaller than the threshold
    df['Affordable Indicator'] = df['Last Sale'].apply(lambda x: 1 if x < last_sale_threshold else 0)

    # Create an empty list to store the moving averages
    moving_averages = []

    # Loop through each stock symbol in the DataFrame and get moving averages
    for symbol in df['Symbol']:
        stock = yf.Ticker(symbol)  # Fetch stock data using yfinance
        data = stock.history(period="200d")  # Get 1 year of historical data

        # Calculate moving averages
        data['50_day_MA'] = data['Close'].rolling(window=50).mean()
        data['100_day_MA'] = data['Close'].rolling(window=100).mean()
        data['200_day_MA'] = data['Close'].rolling(window=200).mean()

        # Calculate the 20-day simple moving average (SMA) and standard deviation
        data['SMA'] = data['Close'].rolling(window=20).mean()
        data['STD'] = data['Close'].rolling(window=20).std()

        # Calculate the lower Bollinger Band (support level)
        data['Lower Band'] = data['SMA'] - (2 * data['STD'])

        # Get the last value of the lower band (support price)
        support_price = data['Lower Band'].iloc[-1]

        # Extract the most recent values of the moving averages
        latest_data = data.iloc[-1]  # Get the last row (most recent data)
        current_price = stock.history(period="1d")['Close'].iloc[-1] # Get the current price

        moving_averages.append({
            'Symbol': symbol,
            '50_day_MA': latest_data['50_day_MA'],
            '100_day_MA': latest_data['100_day_MA'],
            '200_day_MA': latest_data['200_day_MA'],
            'Current Price': current_price,
            'Support Price': support_price
        })

    # Convert the list of moving averages into a DataFrame
    ma_df = pd.DataFrame(moving_averages)

    # Merge the moving averages DataFrame with the original DataFrame
    result_df = pd.merge(df, ma_df, on='Symbol', how='left')

    # Filter out stocks where the current price is not greater than all the moving averages
    result_df = result_df[
        (result_df['Current Price'] > result_df['50_day_MA']) &
        (result_df['Current Price'] > result_df['100_day_MA']) &
        (result_df['Current Price'] > result_df['200_day_MA'])
    ]

    # Save the final DataFrame to CSV
    result_df.to_csv('filtered_stocks.csv', index=False)
    print("Data saved to 'filtered_stocks.csv'")
    print(str(len(result_df)) + ' stocks were found!')

read_and_filter_stocks(market_cap_threshold=250e9, last_sale_threshold=150)




