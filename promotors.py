import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time

# Load environment variables
load_dotenv()

# Set the Google credentials path from environment variable
GOOGLE_CREDS_PATH = os.getenv('GOOGLE_SHEETS_CREDENTIALS', 'credentials/credentials.json')

# Sheet and tab names (hardcoded)
SHEET_NAME = 'Pankaj_Power'
TAB_NAME = 'Promotors'

# Setup Google Sheets authorization
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(GOOGLE_CREDS_PATH, scope)
client = gspread.authorize(creds)

# Open the Google Sheet 'Pankaj_Power'
try:
    spreadsheet = client.open(SHEET_NAME)
    print(f"Using existing sheet: {SHEET_NAME}")
except gspread.exceptions.SpreadsheetNotFound:
    print(f"Sheet '{SHEET_NAME}' not found.")
    exit()

# Check if 'Promotors' tab exists, if not, create it
try:
    sheet = spreadsheet.worksheet(TAB_NAME)
    print(f"Using existing tab: {TAB_NAME}")
except gspread.exceptions.WorksheetNotFound:
    print(f"Tab '{TAB_NAME}' not found. Creating a new tab...")
    sheet = spreadsheet.add_worksheet(title=TAB_NAME, rows="100", cols="20")
    print(f"New tab '{TAB_NAME}' created.")

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
            df_filtered.to_csv('data/nse_filtered_data.csv', index=False)
            print("Filtered data saved to data/nse_filtered_data.csv")

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
