import os
from dotenv import load_dotenv

import clickhouse_connect

from google.oauth2 import service_account
from googleapiclient.discovery import build
import json

import requests

# подгрузка переменных из окружения
load_dotenv()


# подключение к таблице Google Sheets со списком обновляемых таблиц в ClickHouse
# и сохранение названий таблиц и названий полей с датой
creds = os.getenv('creds')
spreadsheet_id = os.getenv('spreadsheet_id')


def get_tables(sheets_range):
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(creds), scopes=["https://www.googleapis.com/auth/spreadsheets"])
    service = build("sheets", "v4", credentials=credentials)

    sheet = service.spreadsheets()
    result = (
        sheet.values()
        .get(spreadsheetId=spreadsheet_id, range=sheets_range)
        .execute()
    )
    values = result.get("values", [])

    return values


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


# проверка пропусков в таблицах ClickHouse из списка от минимальной даты до вчера для таблиц по дням
# и до прошлого месяца для таблиц по месяцам
# и отправка в Телеграм-канал
range_names = json.loads(os.getenv('range_names'))
message_count = 1

for date_range in range(len(range_names)):
    table_values = get_tables(range_names[date_range])

    # дебаг
    print('таблица вся')
    print(table_values)

    # части запроса, отличающиеся между проверкой по дням и месяцам
    if 'Month' in range_names[date_range]:
        interval = '- INTERVAL 1 MONTH'
        start_of_month_str = 'DISTINCT toStartOfMonth'
        table_type = 'Месяцы'
    else:
        interval = ''
        start_of_month_str = ''
        table_type = 'Дни'

    for row in range(len(table_values)):
        
        # дебаг
        print('строка')
        print(table_values[row][0])
        print(table_values[row][1])

        # дебаг первой части
        print(f"""
            SELECT DISTINCT database FROM system.columns 
            WHERE table = '"""+table_values[row][0]+"""' """)[0][0])
        
        database = client.query_np("""
            SELECT DISTINCT database FROM system.columns 
            WHERE table = '"""+table_values[row][0]+"""' """)[0][0]

        #дебаг первой части
        print('запрос выше прошел')

        table = database + '.' + table_values[row][0]
        date_column = table_values[row][1]

        # сохраняем исключения в переменную, если они есть
        if len(table_values[row]) == 3:
            exceptions = ", '" + table_values[row][2]
        else:
            exceptions = ''

        # если таблица сгруппирована по датам, неделям и месяцам,
        # то сохраняем название поля с типом даты
        dateType_column = client.query_np("""
        SELECT name FROM system.columns 
        WHERE table = '"""+table_values[row][0]+"""' AND (lower(name) = 'datetype' OR lower(name) = 'typedate')""")

        # если группировка по разным периодам есть, то проверка пропусков по типу "По дням"
        if dateType_column.size != 0:
            dateType_string = "WHERE "+dateType_column[0][0]+" = 'По дням'"
        else:
            dateType_string = ''

        result = client.query_np("""
        WITH 
            (SELECT toStartOfDay(toDate(MIN("""+date_column+"""))) FROM """+table+""" """+dateType_string+""") AS start,
            toStartOfDay(now()) """+interval+""" AS end
        SELECT """+start_of_month_str+"""(arrayJoin(arrayMap(x -> toDate(x), range(toUInt32(assumeNotNull(start)), toUInt32(end), 24 * 3600)))) dates
        WHERE dates NOT IN
            (SELECT DISTINCT toDate("""+date_column+""")
            FROM """+table+""")
            AND dates NOT IN ('2000-01-01'"""+exceptions+""")""")

        # отправка в Телеграм-канал, если есть пропуски
        if result.size != 0:
            message_text = "{} {}. {}:\n{}".format(
                table_type, message_count, table, result)
            send_telegram_message(bot_token, channel_id, message_text)
            message_count += 1
