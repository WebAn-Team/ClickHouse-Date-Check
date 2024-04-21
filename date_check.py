import os
from dotenv import load_dotenv

import clickhouse_connect

from google.oauth2 import service_account
from googleapiclient.discovery import build
import json

import requests

# подгрузка переменных из окружения
load_dotenv()


# подключение к таблице Google Sheets со списком обновляемых по дням таблиц в ClickHouse
# и сохранение названий таблиц и названий полей с датой
creds = os.getenv('creds')
spreadsheet_id = os.getenv('spreadsheet_id')
date_range_name = os.getenv('date_range_name')

tables = []
date_columns = []


def get_tables():
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(creds), scopes=["https://www.googleapis.com/auth/spreadsheets"])
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


# функция для отправки сообщений в Телеграм
bot_token = os.getenv('bot_token')
channel_id = os.getenv('channel_id')


def send_telegram_message(bot_token, channel_id, message):
    api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    params = {"chat_id": channel_id, "text": message}
    requests.post(api_url, params)


# подключение к ClickHouse
host = os.getenv('host')
username = os.getenv('ch_username')
password = os.getenv('password')

client = clickhouse_connect.get_client(
    host=host, port=8443, username=username, password=password, interface="https")


# проверка пропусков в таблицах ClickHouse из списка от минимальной даты до вчера
# и отправка в Телеграм-канал
message_count = 1

for x in range(len(tables)):
    table = tables[x]
    date_column = date_columns[x]

    # если таблица сгруппирована по датам, неделям и месяцам,
    # то сохраняем название поля с типом даты
    dateType_column = client.query_np("""
    SELECT name FROM system.columns 
    WHERE table = '"""+table+"""' AND (name = 'dateType' OR name = 'typeDate')""")

    # если группировка по разным периодам есть, то проверка пропусков по типу "По дням"
    if (dateType_column.size != 0):
        result = client.query_np("""
        WITH 
            (SELECT toStartOfDay(toDate(MIN("""+date_column+"""))) FROM megafon_dashboards_aggregate."""+table+""" WHERE """+dateType_column[0][0]+""" = 'По дням') AS start,
            toStartOfDay(now()) AS end
        SELECT arrayJoin(arrayMap(x -> toDate(x), range(toUInt32(assumeNotNull(start)), toUInt32(end), 24 * 3600))) dates
        WHERE dates NOT IN
        (SELECT DISTINCT toDate("""+date_column+""")
        FROM megafon_dashboards_aggregate."""+table+""")""")

    # если нет, то просто проверка в поле с датами
    else:
        result = client.query_np("""
        WITH 
            (SELECT toStartOfDay(toDate(MIN("""+date_column+"""))) FROM megafon_dashboards_aggregate."""+table+""") AS start,
            toStartOfDay(now()) AS end
        SELECT arrayJoin(arrayMap(x -> toDate(x), range(toUInt32(assumeNotNull(start)), toUInt32(end), 24 * 3600))) dates
        WHERE dates NOT IN
        (SELECT DISTINCT toDate("""+date_column+""")
        FROM megafon_dashboards_aggregate."""+table+""")""")

    # отправка в Телеграм-канал, если есть пропуски
    if result.size != 0:
        message_text = "{}. {}:\n{}".format(message_count, table, result)
        send_telegram_message(bot_token, channel_id, message_text)
        message_count += 1
