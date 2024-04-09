import os
from dotenv import load_dotenv

import clickhouse_connect

from google.oauth2 import service_account
from googleapiclient.discovery import build
import json

import requests

load_dotenv()



host = os.getenv('host')
username = os.getenv('ch_username')
password = os.getenv('password')

client = clickhouse_connect.get_client(host=host, port=8443, username=username, password=password, interface = "https")

creds = os.getenv('creds')
spreadsheet_id = os.getenv('spreadsheet_id')
month_range_name = os.getenv('month_range_name')

tables = []
date_columns = []

def get_tables():
  credentials = service_account.Credentials.from_service_account_info(json.loads(creds), scopes=["https://www.googleapis.com/auth/spreadsheets"])
  service = build("sheets", "v4", credentials=credentials)

  sheet = service.spreadsheets()
  result = (
      sheet.values()
      .get(spreadsheetId=spreadsheet_id, range=month_range_name)
      .execute()
  )
  values = result.get("values", [])

  for row in values:
      tables.append(row[0])
      date_columns.append(row[1])


if __name__ == "__main__":
  get_tables()

print(tables)
print(len(tables))