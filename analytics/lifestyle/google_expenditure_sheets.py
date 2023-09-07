from __future__ import print_function
import httplib2
import os

import pandas as pd
import os.path
import traceback
import re
from datetime import datetime

from urllib.parse import parse_qs, urlparse

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import unittest
from pprint import pprint

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

#Other scopes
#https://www.googleapis.com/auth/drive
#https://www.googleapis.com/auth/drive.readonly
#https://www.googleapis.com/auth/drive.file
#https://www.googleapis.com/auth/spreadsheets

APPLICATION_NAME = 'Python Interface Exploration'

class Sheets:
    def __init__(self, creds, spreadsheet_id, cache_folder='cache', offline=False):
        if offline:
            print('Working in offline mode. Make sure that the cached files are present.')
        self.creds = creds
        self.spreadsheet_id = spreadsheet_id
        self.offline = offline
        self.service = build('sheets', 'v4', credentials=self.creds) if not offline else None
        self.cache_folder = cache_folder


    def open(self, sheet_names):
        try:
            # Create the cache folder if it doesn't exist
            if not os.path.exists(self.cache_folder):
                os.makedirs(self.cache_folder)
            
            # Dictionary to store DataFrames for each sheet
            df = None
            dataframes = []
            for sheet_name in sheet_names:
                s_df = None
                cache_file = os.path.join(self.cache_folder, f"{sheet_name}.csv")

                # Check if a cached CSV file exists
                if os.path.exists(cache_file):
                    s_df = pd.read_csv(cache_file, index_col="Date")
                    s_df.index = pd.to_datetime(s_df.index)
                    #print(f'Load {cache_file} from cache')
                else:
                    # Fetch data from Google Sheets
                    s_df = self.fetch_data(sheet_name)

                    # Save the fetched data to a CSV file for caching
                    s_df.to_csv(cache_file)

                dataframes.append(s_df)

            df = pd.concat(dataframes, join='outer', sort=True)
            df.sort_index(inplace=True)

            # Now, df contains the combined DataFrame for the specified sheets
            return df

        except Exception as e:
            print(f"Error: {str(e)}")
            traceback.print_exc()
            return None

    def get_sheet_names(self):
        try:
            sheet_metadata = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()
            sheets = sheet_metadata.get('sheets', [])

            sheet_names = [sheet['properties']['title'] for sheet in sheets]
            return sheet_names

        except Exception as e:
            print(f"Error: {str(e)}")
            traceback.print_exc()
            return None
        
    def fetch_data(self, sheet_name):
        range_name = f'{sheet_name}!A:G'  # Adjust columns as needed
        result = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=range_name).execute()
        values = result.get('values', [])

        print(f'Sheet: {sheet_name}')
        if not values:
            print(f'No data found in sheet: {sheet_name}')
            return None
        month = sheet_name.split("'")[0]
        year = int(sheet_name.split("'")[1])+2000

        columns = ['Date','Money spent',
                    'Expense category', 'Spent on', 
                    'Point of Sale/App used', 'Via card/wallet/cash',
                    'Paid by']
        rows = []
        end_reached = False
        for row in values:
            if len(row) < 3:
                if end_reached:
                    break
                end_reached = True #Allow one blank space between entries
                continue
            if row[0].strip() == 'Date':
                #columns = [col.strip() for col in row]
                continue
            elif len(row) == len(columns):
                if len(row[0].strip().split(' '))>1 and len(row[0].strip().split(' '))!=3: #Minor patchwork on date
                    row[0] = f'{row[0].strip()} {year}'
                print(row)
                rows.append(row)
            elif len(row) < len(columns):
                #if row has entry for some category, only then enter it
                if len(row[2].strip())>0:
                    newrow = [col for col in row]
                    newrow += ['' for jj in range(len(row), len(columns))]
                    if len(newrow[0].strip().split(' '))>1 and len(newrow[0].strip().split(' '))!=3: #Minor patchwork on date
                        newrow[0] = f'{newrow[0].strip()} {year}'
                    print(newrow)
                    rows.append(newrow)
            else: #len > len(columns)
                rows.append(row[0:len(columns)])

        print(columns)
        df = pd.DataFrame(rows, columns=columns)

        # Clean and parse the date column
        df["Date"] = df["Date"].apply(self.clean_and_parse_date)
        #print(df.head(10))
        # Parse dates and set them as the index
        df["Date"] = pd.to_datetime(df["Date"])

        # Fill missing dates with the previous valid date
        df['Date'] = df['Date'].fillna(method='ffill')

        df.set_index("Date", inplace=True)
        print(df.head(10))
        return df

    def clean_and_parse_date(self, date_str):
        # Define regex patterns for common date formats
        date_patterns = [
            r'\d{1,2} (?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec) \d{4}',  # 27 Jan 2023
            # Add more patterns for other date formats as needed
        ]

        # Check if the date_str matches any of the patterns
        for pattern in date_patterns:
            match = re.search(pattern, date_str)
            if match:
                matched_date = match.group()
                # Perform further parsing and return a standardized date
                standardized_date = self.parse_date(matched_date)
                return standardized_date

        # If no matching pattern is found, return None (or handle as needed)
        return date_str

    def parse_date(self, date_str):
        # Implement custom logic to parse and standardize date_str
        # You can use datetime.strptime and datetime.strftime for parsing and formatting

        # Example:
        # parsed_date = datetime.datetime.strptime(date_str, "%d %b %Y")
        # standardized_date = parsed_date.strftime("%d %B %Y")
        parsed_date = datetime.strptime(date_str, "%d %b %Y")

        # Use strftime to format the date as '27 January 2023'
        standardized_date = parsed_date.strftime("%d %B %Y")
        # Return the standardized date
        return standardized_date
    
def run_console_hack(flow):
    flow.redirect_uri = 'http://localhost:1'
    auth_url, _ = flow.authorization_url()
    print(
        "Visit the following URL:",
        auth_url,
        "After granting permissions, you will be redirected to an error page",
        "Copy the URL of that error page (http://localhost:1/?state=...)",
        sep="\n"
    )
    redir_url = input("URL: ")
    code = parse_qs(urlparse(redir_url).query)['code'][0]
    flow.fetch_token(code=code)
    return flow.credentials

def get_credentials():
    token = 'token.json'
    creds = None
    if os.path.exists(token):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                                            'credentials.json', SCOPES)
            #creds = flow.run_local_server(port=0)
            creds = run_console_hack(flow)
        # Save the credentials for the next run
        with open(token, 'w') as tokenfile:
            tokenfile.write(creds.to_json())
    return creds

def reclassify(df):
    # Define a dictionary to map keywords to categories
    keyword_to_category = {
        'Milk': 'Groceries',
        'Curd': 'Groceries',
        'Rent': 'Household',
        'Vegetables': 'Groceries',
        'Fruits': 'Groceries',
        'Eggs': 'Groceries',
        'Flour': 'Groceries',
        'Book' : 'Books',
        # Add more keyword-category mappings as needed
    }
    # Iterate through the DataFrame and update 'Expense category' based on keywords
    for keyword, category in keyword_to_category.items():
        df.loc[df['Spent on'].str.contains(keyword, case=False, na=False), 'Expense category'] = category

    #Additional reclassification for specific cases
    # Reclassify entries within 'Transport' category based on keywords
    transport_keywords = ['IIT', 'office', 'college', 'auto', 'uber', 'rapido', 'ola']  # Add more keywords as needed
    for keyword in transport_keywords:
        df.loc[(df['Expense category'] == 'Transport') & (df['Spent on'].str.contains(keyword, case=False, na=False)), 'Expense category'] = 'Local Transport'
    return df

def main(offline=False):
    creds = None
    if not offline:
        creds = get_credentials()
    
    gsheet = Sheets(creds=creds, spreadsheet_id='<id>', offline=offline)
    #sheet_names = gsheet.get_sheet_names()
    sheet_names = ["September'23", "August'23", "July'23", "June'23", "May'23", "April'23",
                    "March'23", "February'23", "January'23", "December'22", "September'22", 
                    "August'22", "July'22", "June'22", "May'22", "April'22", "March'22", 
                    "February'22", "January'22", "December'21", "November'21", "October'21", 
                    "September'21", "August'21", "July'21", "Jun '21", "May'21", "April'21", 
                    "March'21", "February'21", "January'21", "December'20", "November'20", 
                    "October'20", "September'20", "August'20", "July'20", "June'20", "May'20", 
                    "April'20", "March'20", "February'20", "January'20", "December'19", 
                    "November'19", "October'19", "September'19"]
    #print(sheet_names)

    df = gsheet.open(sheet_names=sheet_names)

    df = reclassify(df)
    # Group the DataFrame by 'Expense category' and calculate the sum of 'Money spent'
    expense_summary = df.groupby('Expense category')['Money spent'].sum().reset_index()

    # Rename the 'Money spent' column to 'Total spent'
    expense_summary.rename(columns={'Money spent': 'Total spent'}, inplace=True)

    # Sort the summary DataFrame by 'Total spent' in descending order
    expense_summary = expense_summary.sort_values(by='Total spent', ascending=False)

    # Print the summary
    print(expense_summary)

    #Monthly summaries
    # Extract the month and year from the 'Date' column
    df['YearMonth'] = df.index.strftime('%Y-%m')

    # Group the data by 'YearMonth' and calculate the total expenses for each month
    monthly_summary = df.groupby('YearMonth')['Money spent'].sum().reset_index()

    # Rename the columns for clarity
    monthly_summary.columns = ['Month', 'Total Expenses']

    # Display the summary
    print(monthly_summary)

    # Get unique months
    unique_months = df['YearMonth'].unique()

    # Iterate through each unique month and display the summary
    for month in unique_months:
        monthly_data = df[df['YearMonth'] == month]
        monthly_category_summary = monthly_data.groupby(['Expense category'])['Money spent'].sum().reset_index()
        monthly_category_summary.columns = ['Expense Category', 'Total Expenses']
        
        print(f"Summary for {month}:")
        print(monthly_category_summary)
        print("\n")
    
    # Combine all 'Spent on' entries into a single text string
    all_spent_on_text = ', '.join(df['Spent on'].dropna())

    # Split the text string by commas to extract keywords
    keywords = [keyword.strip() for keyword in all_spent_on_text.split(',')]

    # Create a set of unique keywords
    unique_keywords = set(keywords)

    # Print the unique keywords
    print('Unique keywords:')
    print(unique_keywords)

    # Filter the DataFrame for 'Personal' expenses
    personal_expenses = df[df['Expense category'] == 'Personal']

    # Sort the 'Personal' expenses by 'Money spent' in descending order
    personal_expenses_sorted = personal_expenses.sort_values(by='Money spent', ascending=False)

    # Display the sorted 'Personal' expenses
    print(personal_expenses_sorted)

if __name__ == "__main__":
    main(offline=True)

