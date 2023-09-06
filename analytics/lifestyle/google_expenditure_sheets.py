'''
This script is based on https://developers.google.com/sheets/api/quickstart/python

It is aimed to read into Google Spreadsheets and manipulate/pull data from there.
'''
from __future__ import print_function
import httplib2
import os

import pandas as pd
import os.path

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
    def __init__(self, creds, spreadsheet_id):
        self.creds = creds
        self.spreadsheet_id = spreadsheet_id
        self.service = build('sheets', 'v4', credentials=self.creds)

    def open(self, sheet_names):
        try:
            # Dictionary to store DataFrames for each sheet
            df = None

            for sheet_name in sheet_names:
                if df is None:
                    df = self.fetch_data(sheet_name)
                else:
                    s_df = self.fetch_data(sheet_name)
                    df = pd.concat([df, s_df], join='outer', sort=True)
                    df.sort_index(inplace=True)

            # Now, df contains the combined DataFrame for the specified sheets
            return df

        except Exception as e:
            print(f"Error: {str(e)}")
            return None

    def fetch_data(self, sheet_name):
        range_name = f'{sheet_name}!A:G'  # Adjust columns as needed
        result = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=range_name).execute()
        values = result.get('values', [])

        if not values:
            print(f'No data found in sheet: {sheet_name}')
            return None

        ii = 0
        for row in values:
            if ii == 0:
                for col in range(0, len(row)):
                    row[col] = row[col].strip()
            if len(row) < 3:
                break
            ii += 1

        df = pd.DataFrame(values[1:ii], columns=values[0])

        # Parse dates and set them as the index
        df["Date"] = pd.to_datetime(df["Date"])

        # Fill missing dates with the previous valid date
        df['Date'] = df['Date'].fillna(method='ffill')

        df.set_index("Date", inplace=True)

        return df

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
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(token, 'w') as tokenfile:
            tokenfile.write(creds.to_json())
    return creds

def main():
    creds = get_credentials()
    gsheet = Sheets(creds=creds, spreadsheet_id='<id>')
    df = gsheet.open(sheet_names=["August'23", "July'23"])
    
    print(df.head(20))
    

if __name__ == "__main__":
    main()

