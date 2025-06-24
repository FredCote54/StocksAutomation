from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.chrome.options import Options
import time
import os
import pandas as pd
import random





#############################################################################
####################        CHOOSE PARAMETERS       #########################
#############################################################################


RUN_download_stocks = False
RUN_filter_stocks = True
RUN_testing = False

#############################################################################
#############################################################################

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

def get_moving_avg(tickers, headless=True):
    delay_range = (2, 4)
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("window-size=1920,1080")
    options.add_argument("user-agent=Mozilla/5.0")

    driver = webdriver.Chrome(options=options)
    results = []

    for i, symbol in enumerate(tickers):
        print(f"({i+1}/{len(tickers)}) Fetching {symbol}...")

        data = {
            "Symbol": symbol,
            "MA_50": None,
            "MA_100": None,
            "MA_200": None,
            "Current Price": None
        }

        try:
            # Scrape technical analysis page
            driver.get(f"https://www.barchart.com/stocks/quotes/{symbol}/technical-analysis")
            time.sleep(random.uniform(*delay_range))

            rows = driver.find_elements(By.CSS_SELECTOR, ".analysis-table-wrapper table tbody tr")
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) >= 2:
                    period = cols[0].text.strip()
                    value = cols[1].text.strip().replace(",", "")
                    try:
                        value = float(value)
                    except ValueError:
                        continue

                    if "50-Day" in period:
                        data["MA_50"] = value
                    elif "100-Day" in period:
                        data["MA_100"] = value
                    elif "200-Day" in period:
                        data["MA_200"] = value

            # Scrape overview page for Current Price
            driver.get(f"https://www.barchart.com/stocks/quotes/{symbol}/overview")
            time.sleep(random.uniform(*delay_range))
            price_span = driver.find_element(By.CSS_SELECTOR, ".pricechangerow .last-change")
            data["Current Price"] = float(price_span.text.replace(",", ""))

        except Exception as e:
            print(f"❌ Failed to fetch {symbol}: {e}")

        results.append(data)

    driver.quit()
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

    df = df[
        (df['Current Price'] > df['MA_50']) &
        (df['Current Price'] > df['MA_100']) &
        (df['Current Price'] > df['MA_200'])
    ]

    print(f"Remaining rows after averages: {len(df)}")

    return  df


if RUN_download_stocks:
    download_stocks_csv()

if RUN_filter_stocks:
    read_and_filter_stocks(250e9, 150)


if RUN_testing:
    print('fern')
