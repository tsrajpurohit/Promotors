import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import gspread
from google.oauth2.service_account import Credentials
import time
import json

# Load environment variables
load_dotenv()

# Define the scope for Google Sheets API
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Load credentials from the environment variable
credentials_json = os.getenv('GOOGLE_SHEETS_CREDENTIALS')

if not credentials_json:
    raise ValueError("No credentials found. Please set the GOOGLE_SHEETS_CREDENTIALS environment variable.")

# Parse the JSON string from environment variable and load credentials
try:
    credentials_info = json.loads(credentials_json)
    credentials = Credentials.from_service_account_info(credentials_info, scopes=scope)
except json.JSONDecodeError as e:
    raise ValueError(f"Failed to parse JSON from GOOGLE_SHEET_CREDENTIALS: {e}")

# Authorize the client
client = gspread.authorize(credentials)

# Create or open the Google Sheet
try:
    spreadsheet = client.open("Pankaj_Power")
except gspread.SpreadsheetNotFound:
    spreadsheet = client.create("Pankaj_Power")

# Access the specific worksheet (tab)
try:
    sheet = spreadsheet.worksheet("Promotors")
except gspread.WorksheetNotFound:
    print(f"Tab 'Promotors' not found. Creating it...")
    sheet = spreadsheet.add_worksheet(title="Promotors", rows="100", cols="20")

# Example: print the first row to verify everything is working
print(sheet.row_values(1))

# Sheet and tab names (hardcoded)
SHEET_NAME = 'Pankaj_Power'
TAB_NAME = 'Promotors'

# Date range setup
fromdate = datetime.strftime(datetime.today(), '%d-%m-%Y')
todate = datetime.today() - timedelta(days=120)
enddate = datetime.strftime(todate, '%d-%m-%Y')

# URL with the date range
url = f'https://www.nseindia.com/api/corporates-pit?index=equities&from_date={enddate}&to_date={fromdate}'
print(f"URL: {url}")

# Headers for the request
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Referer': 'https://www.nseindia.com',
    'X-Requested-With': 'XMLHttpRequest'
}

# Initialize a session
session = requests.Session()
session.headers.update(headers)

# Initial request to get the cookies
try:
    session.get('https://www.nseindia.com', timeout=5)
    time.sleep(1)  # Add delay to avoid getting blocked
except requests.exceptions.RequestException as e:
    print(f"Failed to initialize session: {e}")
    exit()

# Fetch the data
try:
    response = session.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
except requests.exceptions.RequestException as e:
    print(f"Request failed: {e}")
    exit()
except ValueError as e:
    print(f"Failed to parse JSON: {e}")
    exit()

# Extract and process the data
if 'data' in data and isinstance(data['data'], list):
    df = pd.json_normalize(data['data'])

    print(f"Columns: {df.columns}")
    print(df.head())

    # Filter rows where 'tdpTransactionType' is 'buy' or 'sell'
    df_filtered = df[df['tdpTransactionType'].str.strip().str.lower().isin(['buy', 'sell'])]

    # Additional filter: 'secAcq' > 25 lakh (2,500,000)
    if 'secAcq' in df_filtered.columns:
        df_filtered.loc[:, 'secAcq'] = pd.to_numeric(df_filtered['secAcq'], errors='coerce')
        df_filtered = df_filtered[df_filtered['secAcq'] > 2500000]

        if not df_filtered.empty:
            # Save the filtered data to CSV
            df_filtered.to_csv('nse_filtered_data.csv', index=False)
            print("Filtered data saved to nse_filtered_data.csv")

            # Resize the sheet to accommodate the filtered data size
            num_rows, num_cols = df_filtered.shape
            sheet.resize(rows=num_rows + 1, cols=num_cols + 2)  # +1 for header, +1 for timestamp column

            # Write filtered data to the 'Promotors' tab in Google Sheet
            try:
                sheet.clear()  # Clear existing data in the tab
                sheet.update([df_filtered.columns.values.tolist()] + df_filtered.values.tolist())
                print(f"Filtered data uploaded to Google Sheet, tab: '{TAB_NAME}'")

                # Add the timestamp in the last column
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                last_column = len(df_filtered.columns)  # Last column of the data

                # Update the header row with 'Last Updated' in the last column
                sheet.update_cell(1, last_column + 1, "Last Updated")

                # Update the second row with the timestamp in the last column
                sheet.update_cell(2, last_column + 1, timestamp)

                print(f"Timestamp '{timestamp}' added to the last column of the sheet.")
                
            except Exception as e:
                print(f"Failed to upload data to Google Sheet: {e}")
        else:
            print("No data found matching the criteria ('buy'/'sell' and 'secAcq' > 25 lakh).")
    else:
        print("'secAcq' column not found in the data.")
else:
    print("Unexpected data structure or key not found.")
