'''
This script is based on https://developers.google.com/sheets/api/quickstart/python

It is aimed to read into Google Spreadsheets and manipulate/pull data from there.
'''
from __future__ import print_function
import httplib2
import os

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

import unittest
from pprint import pprint

try:
  import argparse
  flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
  flags = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/sheets.googleapis.com-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

#Other scopes
#https://www.googleapis.com/auth/drive
#https://www.googleapis.com/auth/drive.readonly
#https://www.googleapis.com/auth/drive.file
#https://www.googleapis.com/auth/spreadsheets


CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Python Interface Exploration'

def get_credentials(open_mode):
  """Gets valid user credentials from storage.

  If nothing has been stored, or if the stored credentials are invalid,
  the OAuth2 flow is completed to obtain the new credentials.

  Returns:
      Credentials, the obtained credential.
    """
  home_dir = os.path.expanduser('~')
  credential_dir = os.path.join(home_dir, '.credentials')
  if not os.path.exists(credential_dir):
    os.makedirs(credential_dir)
  credential_path = os.path.join(credential_dir,
                                   'sheets.googleapis.com-python-quickstart.json')

  store = Storage(credential_path)
  credentials = store.get()
  if not credentials or credentials.invalid:
    flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, open_mode)
    flow.user_agent = APPLICATION_NAME
    if flags:
      credentials = tools.run_flow(flow, store, flags)
    else: # Needed only for compatibility with Python 2.6
      credentials = tools.run(flow, store)
      
    print('Storing credentials to ' + credential_path)
  return credentials

class Tutorial(unittest.TestCase):
    discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
                    'version=v4')
    def setUp(self):
      """Setup basic authentication to server
      """
      self.credentials = get_credentials(SCOPES)
      self.http = self.credentials.authorize(httplib2.Http())
      self.service = discovery.build('sheets', 'v4', http=self.http,
                                       discoveryServiceUrl = self.discoveryUrl)
                                       
    def tearDown(self):
      pass
       
    def test_open_spreadsheet_readonly(self):
      """
      Query all sheet names in the spreadsheet
      """
      spreadsheet_id = '1T3qmnmPugaDfQfkhMGITba1tiBuA_AfcNwKHd4HGc3U'
      collection = self.service.spreadsheets() #Spreadsheets collection
      #collection = self.service.spreadsheets().values() #Nested Collection
      request = collection.get(spreadsheetId=spreadsheet_id)
      #request is an HttpRequest object
      response = request.execute()
      
      #Response is a dictionary
      #pprint(response)
      sheets = response['sheets']
      for sheet in sheets:
        properties = sheet['properties']
        print(properties['title'])
        

if __name__ == "__main__":
    unittest.main()

