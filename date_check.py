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
client_cert = os.getenv('client_cert')

client = clickhouse_connect.get_client(host=host, port=8443, username=username, password=password, interface = "https", ca_cert = client_cert)

creds = os.getenv('creds')
spreadsheet_id = os.getenv('spreadsheet_id')
date_range_name = os.getenv('date_range_name')
client_cert = ".yandex\RootCA.crt"

tables = []
date_columns = []

def get_tables():
  credentials = service_account.Credentials.from_service_account_info(json.loads(creds), scopes=["https://www.googleapis.com/auth/spreadsheets"])
  service = build("sheets", "v4", credentials=credentials)

  sheet = service.spreadsheets()
  result = (
      sheet.values()
      .get(spreadsheetId=spreadsheet_id, range=date_range_name)
      .execute()
  )
  values = result.get("values", [])

  for row in values:
      tables.append(row[0])
      date_columns.append(row[1])


if __name__ == "__main__":
  get_tables()



bot_token = os.getenv('bot_token')
channel_id = os.getenv('channel_id')

def send_telegram_message(bot_token, channel_id, message):
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    params = {"chat_id": channel_id, "text": message}
    requests.post(api_url, params)



for x in range(len(tables)):
    table = tables[x]
    date_column = date_columns[x]

    result = client.query_np("""
    WITH 
        (SELECT toStartOfDay(toDate(MIN("""+date_column+"""))) FROM megafon_dashboards_aggregate."""+table+""") AS start,
        toStartOfDay(now()) AS end
    SELECT arrayJoin(arrayMap(x -> toDate(x), range(toUInt32(assumeNotNull(start)), toUInt32(end), 24 * 3600))) dates
    WHERE dates NOT IN
    (SELECT DISTINCT toDate("""+date_column+""")
    FROM megafon_dashboards_aggregate."""+table+""")""")

    message_text = "{}:\n{}".format(table, result)
    send_telegram_message(bot_token, channel_id, message_text)