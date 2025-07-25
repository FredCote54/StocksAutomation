from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as ec
from selenium.webdriver.chrome.options import Options
import re
import time
import os
import pandas as pd
import random
from bs4 import BeautifulSoup
import requests
import urllib.parse
import math
from tqdm import tqdm




#############################################################################
####################        CHOOSE PARAMETERS       #########################
#############################################################################

options_date = '2025-08-15'

RUN_download_stocks = False
RUN_filter_stocks = False
RUN_beautify = True
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

def extract_barchart_last_price(soup_text):
    match = re.search(r'"lastPrice":"([\d.]+)"', soup_text)
    if match:
        return float(match.group(1))
    else:
        print("⚠️ dailyLastPrice not found in soup.")
        return None

def get_moving_avg(tickers):
    headers = {
        "User-Agent": "Mozilla/5.0"
    }
    delay_range = (1, 2.5)

    results = []

    for i, symbol in enumerate(tickers):
        print(f"({i+1}/{len(tickers)}) Fetching {symbol}...")

        data = {
            "Symbol": symbol,
            "MA_20": None,
            "HV_20": None,
            "HV_50": None,
            "MA_50": None,
            "MA_100": None,
            "MA_200": None,
            "Floor_20_1": None,
            "Floor_20_2": None,
            "Floor_20_3": None,
            "Floor_50_1": None,
            "Floor_50_2": None,
            "Floor_50_3": None,
            "Current Price": None
        }

        try:
            # Moving Averages from Barchart
            url_bc = f"https://www.barchart.com/stocks/quotes/{symbol}/technical-analysis"
            resp_bc = requests.get(url_bc, headers=headers, timeout=10)
            soup_bc = BeautifulSoup(resp_bc.text, "html.parser")

            all_tables = soup_bc.find_all("div", class_="analysis-table-wrapper")

            if len(all_tables) >= 1:
                ma_table = all_tables[0].find("table")
                if ma_table:
                    rows = ma_table.find_all("tr")
                    for row in rows:
                        cols = row.find_all("td")
                        if len(cols) >= 2:
                            period = cols[0].text.strip()
                            value = cols[1].text.strip().replace(",", "")
                            try:
                                value = float(value)
                            except ValueError:
                                continue
                            if "20-Day" in period:
                                data["MA_20"] = value
                            elif "50-Day" in period:
                                data["MA_50"] = value
                            elif "100-Day" in period:
                                data["MA_100"] = value
                            elif "200-Day" in period:
                                data["MA_200"] = value

            if len(all_tables) >= 3:
                hv_table = all_tables[2].find("table")
                if hv_table:
                    rows = hv_table.find_all("tr")
                    for row in rows:
                        cols = row.find_all("td")
                        if len(cols) >= 2:
                            period = cols[0].text.strip()
                            value = cols[3].text.strip().replace("%", "")
                            try:
                                value = float(value)
                            except ValueError:
                                continue
                            if "20-Day" in period:
                                data["HV_20"] = value
                            elif "50-Day" in period:
                                data["HV_50"] = value


            # Compute the floor if both values are available
            if data["MA_20"] and data["HV_20"]:
                hv_daily_20 = (data["HV_20"]/100) / math.sqrt(252)
                data["Floor_20_1"] = round(data["MA_20"] * (1 - 1 * hv_daily_20), 2)
                data["Floor_20_2"] = round(data["MA_20"] * (1 - 2 * hv_daily_20), 2)
                data["Floor_20_3"] = round(data["MA_20"] * (1 - 3 * hv_daily_20), 2)

            if data["MA_50"] and data["HV_50"]:
                hv_daily_50 = (data["HV_50"]/100) / math.sqrt(252)
                data["Floor_50_1"] = round(data["MA_50"] * (1 - 1 * hv_daily_50), 2)
                data["Floor_50_2"] = round(data["MA_50"] * (1 - 2 * hv_daily_50), 2)
                data["Floor_50_3"] = round(data["MA_50"] * (1 - 3 * hv_daily_50), 2)

            soup_text = resp_bc.text
            last_price = extract_barchart_last_price(soup_text)
            if last_price:
                data["Current Price"] = float(last_price)

        except Exception as e:
            print(f"❌ Failed to fetch {symbol}: {e}")

        results.append(data)
        time.sleep(random.uniform(*delay_range))

    return pd.DataFrame(results)

def get_barchart_tokens():
    options = Options()
    # Avoid headless mode to ensure tokens load correctly
    # options.add_argument("--headless")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--start-maximized")

    driver = webdriver.Chrome(options=options)

    driver.get("https://www.barchart.com/stocks/quotes/AAPL/options")
    time.sleep(10)  # Wait to ensure all cookies are set

    priority_order = ['market', 'bcFreeUserPageView', 'webinarClosed', 'laravel_token',
                      'laravel_session', 'XSRF-TOKEN']

    cookies = driver.get_cookies()
    cookie_dict = {cookie['name']: cookie['value'] for cookie in cookies}

    priority_order = [
        'market',
        'bcFreeUserPageView',
        'webinarClosed',
        'laravel_token',
        'XSRF-TOKEN',
        'laravel_session',
    ]

    # Add priority cookies first, in defined order
    seen = set()
    cookie_items = []
    for name in priority_order:
        if name in cookie_dict:
            cookie_items.append(f"{name}={cookie_dict[name]}")
            seen.add(name)

    # Add the remaining cookies in original order, excluding those already added
    for cookie in cookies:
        name = cookie['name']
        if name not in seen:
            cookie_items.append(f"{name}={cookie['value']}")
            seen.add(name)

    cookie_str = "; ".join(cookie_items)

    # Decode XSRF-TOKEN
    xsrf_token_raw = cookie_dict.get('XSRF-TOKEN')
    xsrf_token = urllib.parse.unquote(xsrf_token_raw) if xsrf_token_raw else None

    driver.quit()

    return cookie_str, xsrf_token

def get_barchart_put_options(symbol, expiration, cookie_str, token_str, target_strike=None):
    url = "https://www.barchart.com/proxies/core-api/v1/options/get"
    params = {
        "baseSymbol": symbol,
        "expirationDate": expiration,
        "expirationType": "monthly",
        "groupBy": "optionType",
        "orderBy": "strikePrice",
        "orderDir": "asc",
        "optionsOverview": "true",
        "raw": "1",
        "fields": "symbol,baseSymbol,strikePrice,expirationDate,moneyness,bidPrice,midpoint,askPrice,lastPrice,priceChange,percentChange,volume,openInterest,openInterestChange,volatility,delta,optionType,daysToExpiration,tradeTime,averageVolatility,historicVolatility30d,baseNextEarningsDate,dividendExDate,baseTimeCode,expirationType,impliedVolatilityRank1y,symbolCode,symbolType"
    }

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Cookie": cookie_str,
        "X-XSRF-TOKEN": token_str
    }

    response = requests.get(url, headers=headers, params=params)

    if not response.ok:
        print(f"❌ Request failed: {response.status_code}")
        print(response.text[:500])
        return pd.DataFrame()

    data = response.json()
    puts = data.get("data", {}).get("Put", [])

    if not puts:
        print("⚠️ No put options found.")
        return pd.DataFrame()

    df = pd.DataFrame(puts)

    if target_strike is not None:
        df["strikePrice"] = pd.to_numeric(df["strikePrice"], errors="coerce")
        df = df.dropna(subset=["strikePrice"])

        # Initialize a list to keep all the row indices to retain
        indices_to_keep = set()

        for label, strike_val in target_strike.items():
            # Find index of the row with strike closest to this strike_val
            idx = (df["strikePrice"] - strike_val).abs().idxmin()

            # Create a column for this label, default to 0
            df[label] = 0
            df.at[idx, label] = 1

            indices_to_keep.add(idx)

        # Keep only the rows that were selected for at least one floor
        df = df.loc[list(indices_to_keep)]

    return df

def enrich_df_with_put_options(df, exp_date):
    cookie_str, token_str = get_barchart_tokens()
    enriched_data = []

    for _, row in tqdm(df.iterrows(), total=len(df)):
        symbol = row["Symbol"]
        target_strike = {
            "Floor_20_1_Tag": row["Floor_20_1"],
            "Floor_20_2_Tag": row["Floor_20_2"],
            "Floor_20_3_Tag": row["Floor_20_3"],
            "Floor_50_1_Tag": row["Floor_50_1"],
            "Floor_50_2_Tag": row["Floor_50_2"],
            "Floor_50_3_Tag": row["Floor_50_3"],
        }

        try:
            options_df = get_barchart_put_options(symbol, exp_date, cookie_str, token_str, target_strike=target_strike)

            if options_df is not None and not options_df.empty:
                # Keep core option columns
                base_columns = ["baseSymbol", "strikePrice", "bidPrice", "askPrice", "delta", "volatility"]

                # Detect all tag columns dynamically (e.g., "Floor_20_1", etc.)
                tag_columns = [col for col in options_df.columns if col.startswith("Floor_")]

                # Subset the DataFrame
                selected_rows = options_df[base_columns + tag_columns]

                # Append all selected rows
                enriched_data.extend(selected_rows.to_dict("records"))

            time.sleep(1)
        except Exception as e:
            print(f"Error for {symbol}: {e}")
            time.sleep(1)

    enriched_df = pd.DataFrame(enriched_data)
    merged = df.merge(enriched_df, left_on="Symbol", right_on="baseSymbol", how="left")

    return merged

def read_and_filter_stocks(expiration_date, market_cap_threshold=2e9, last_sale_threshold=150, profit_target=0.01):
    """
    Reads the CSV file in the fixed download directory, filters stocks by market cap,
    and calculates an indicator for "Last Sale".

    Args:
        profit_target(float): minimum profit we want in options
        expiration_date (date): Date of the expiration of options
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

    df = df[
        (df['Current Price'] > df['MA_50']) &
        (df['Current Price'] > df['MA_100']) &
        (df['Current Price'] > df['MA_200'])
    ]

    print(f"Remaining rows after below Moving Averages: {len(df)}")

    df_w_options = enrich_df_with_put_options(df, expiration_date)

    df_w_options = df_w_options[df_w_options["bidPrice"].notna()]

    print(f"Remaining unique stocks: {df_w_options['Symbol'].nunique()}")

    print(f"Number of total options rows that we have : {len(df_w_options)}")

    floor_val_cols = [col for col in df_w_options.columns if col.startswith("Floor_") and not col.endswith("Tag")]
    tag_cols = [col for col in df_w_options.columns if
                col.endswith("Tag") and col.replace("_Tag", "") in floor_val_cols]

    expanded_rows = []

    for _, row in df_w_options.iterrows():
        for tag_col in tag_cols:
            if row.get(tag_col) == 1:
                base_col = tag_col.replace("_Tag", "")
                floor_value = row.get(base_col)

                # Keep only relevant columns
                clean_row = row.drop(floor_val_cols + tag_cols).copy()
                clean_row["Floor Tag"] = base_col
                clean_row["Floor Value"] = floor_value
                expanded_rows.append(clean_row)

    # Step 4: Create the final cleaned DataFrame
    df_cleaned = pd.DataFrame(expanded_rows)


    df_cleaned["Profitability"] = (( pd.to_numeric(df_cleaned["bidPrice"], errors="coerce")+ pd.to_numeric(df_cleaned["askPrice"], errors="coerce")) / 2) / pd.to_numeric(df_cleaned["strikePrice"], errors="coerce")

    df_cleaned = df_cleaned[df_cleaned["Profitability"] >= profit_target]

    df_cleaned = df_cleaned[pd.to_numeric(df_cleaned["delta"], errors="coerce") >= -0.15]

    df_cleaned = df_cleaned[pd.to_numeric(df_cleaned["bidPrice"], errors="coerce") != 0]

    df_cleaned = df_cleaned[(pd.to_numeric(df_cleaned["askPrice"], errors="coerce") / pd.to_numeric(df_cleaned["bidPrice"], errors="coerce")) < 1.5]

    print(f"Remaining rows after removing options that do not meet criteria: {len(df_cleaned)}")

    return  df_cleaned

def beautify_csv(csv_path, attributes, output_path='stocks_output.html'):
    # Read CSV
    df = pd.read_csv(csv_path)

    # Filter columns
    df = df[attributes]

    # Format column names (title case with underscores replaced)
    df.columns = [col.replace('_', ' ').title() for col in df.columns]

    # Format 'Profitability' as percentage if present
    for col in df.columns:
        if 'Profitability' in col:
            df[col] = df[col].apply(lambda x: f"{x * 100:.3f}%")

    df["Profitability_float"] = df["Profitability"].str.rstrip('%').astype(float)
    df = df.sort_values(by=["Floor Tag", "Profitability_float"], ascending=[True, False])
    df = df.drop(columns=["Profitability_float"])

    # Round other numerical columns to 2 decimals
    for col in df.select_dtypes(include='number').columns:
        if col not in df.columns:  # skip already-formatted Profitability
            df[col] = df[col].round(2)

    title_text = "$$$ P-A is about to make it rain $$$"
    subtitle_text = f"{options_date} put options"

    header_html = f"""
    <div class="header-image">
        <img src="Ray_Lewis.png" alt="Header Image">
    </div>
    <div class="page-title">
        <h1>{title_text}</h1>
        <h2>{subtitle_text}</h2>
    </div>
    """
    # Export to HTML with styling
    html = header_html + df.to_html(index=False, border=0, classes='clean-table')

    # Styling: centered text + light blue header
    css = """
    <style>
    .clean-table {
        font-family: Arial, sans-serif;
        border-collapse: collapse;
        width: 100%;
        text-align: center;
    }
    .clean-table td, .clean-table th {
        border: 1px solid #ddd;
        padding: 8px;
        text-align: center;
    }
    .clean-table tr:nth-child(even) {background-color: #f9f9f9;}
    .clean-table tr:hover {background-color: #f1f1f1;}
    .clean-table th {
        padding-top: 12px;
        padding-bottom: 12px;
        background-color: #5bc0de;
        color: white;
        text-align: center;
    }
    header-image {
        text-align: right;
        margin-bottom: 10px;
    }
    .header-image img {
        max-width: 300px;
        height: auto;
        border-radius: 8px;
    }
    
    .page-title {
        text-align: center;
        margin-bottom: 10px;
    }
    .page-title h1 {
        font-size: 28px;
        margin: 0;
        color: #333;
    }
    .page-title h2 {
        font-size: 18px;
        font-weight: normal;
        color: #666;
        margin-top: 5px;
    }
    </style>
    """

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write(css + html)

    print(f"✅ Cleaned table saved to {output_path}")

if RUN_download_stocks:
    download_stocks_csv()

if RUN_filter_stocks:
    stocks_data = read_and_filter_stocks(options_date, 3e9, 150, 0.01)

    stocks_data.to_csv('stocks_data.csv', index=False)

if RUN_beautify:
    print_columns = [
        'Symbol',
        'Current Price',
        'Floor Tag',
        'Floor Value',
        'strikePrice',
        'bidPrice',
        'askPrice',
        'delta',
        'volatility',
        'Profitability'
    ]
    beautify_csv('stocks_data.csv', print_columns)

if RUN_testing:
    print('Fern')
