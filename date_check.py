import logging
import traceback
from typing import Dict, List, Optional, Tuple

from clickhouse_connect.driver.client import Client
from clickhouse_connect.driver.exceptions import DatabaseError
import requests

import config
from clickhouse_client import get_clickhouse_client, build_gap_query
from sheets_client import fetch_all_table_configs
from notifications import send_yandex_message

logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.ERROR)


def is_checked(val: str) -> bool:
    return val.strip().lower() == "true"


def get_col(row: List[str], index: int) -> str:
    return row[index] if len(row) > index else ""


def get_exceptions(val: str) -> List[str]:
    if not val:
        return []
    return [x.strip(" '") for x in val.split(",") if x.strip(" '")]


def check_table_gaps(
    client: Client,
    table_name: str,
    date_column: str,
    exc_list: List[str],
    table_type: str,
    message_count: int,
    oauth_token: str,
    chat_id: str,
    dry_run: bool,
    metadata: Dict[str, Dict[str, Optional[str]]],
) -> bool:
    """
    Проверяет пропуски дат в одной таблице.
    Возвращает True, если найдены пропуски и отправлено сообщение, иначе False.
    """
    table_meta = metadata.get(table_name)
    if not table_meta:
        logging.warning("Skipping table '%s' (not found in system.columns)", table_name)
        return False

    database = table_meta["database"]
    date_type_col = table_meta["date_type_col"]
    table = f"{database}.{table_name}"

    try:
        sql, parameters = build_gap_query(
            table,
            date_column,
            date_type_col,
            table_type,
            exc_list,
        )
        gaps = client.query_np(sql, parameters=parameters)

        if gaps.size != 0:
            if len(gaps) > 11:
                formatted_gaps = "\n".join(
                    [str(x) for x in gaps[:5]] + ["..."] + [str(x) for x in gaps[-5:]]
                )
            else:
                formatted_gaps = "\n".join([str(x) for x in gaps])
            message_text = f"{message_count}. {table_type}. {table}:\n{formatted_gaps}"
            send_yandex_message(oauth_token, chat_id, message_text, dry_run=dry_run)
            return True
        else:
            logging.info("Table '%s': no gaps found for %s.", table, table_type)
            return False

    except DatabaseError as e:
        logging.error(
            "Table '%s' (%s): ClickHouse query error — %s", table_name, table_type, e
        )
    except requests.exceptions.RequestException as e:
        logging.error(
            "Table '%s' (%s): failed to send Yandex Messenger notification — %s",
            table_name,
            table_type,
            e,
        )
    except Exception as e:
        logging.error(
            "Table '%s' (%s): unexpected error — %s\n%s",
            table_name,
            table_type,
            e,
            traceback.format_exc(),
        )

    return False


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler("date_check.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )
    logging.info(
        "Yandex Messenger send mode: %s",
        "DRY RUN (no messages sent)" if config.dry_run else "LIVE (sending to chat)",
    )


def parse_sheet_data(
    range_values_map: Dict[str, List[List[str]]], range_names: List[str]
) -> Tuple[List[Dict], List[str]]:
    all_table_data = []
    table_names = []

    for range_name in range_names:
        table_values = range_values_map.get(range_name, [])
        if (
            table_values
            and table_values[0]
            and table_values[0][0].lower() == "название таблицы"
        ):
            table_values = table_values[1:]

        for row_data in table_values:
            if not row_data or len(row_data) < 2:
                continue

            table_name = row_data[0]
            date_col = row_data[1]
            if table_name not in table_names:
                table_names.append(table_name)

            if is_checked(get_col(row_data, 2)):
                all_table_data.append(
                    {
                        "table_name": table_name,
                        "date_column": date_col,
                        "table_type": "Дни",
                        "exc_list": get_exceptions(get_col(row_data, 3)),
                    }
                )

            if is_checked(get_col(row_data, 4)):
                all_table_data.append(
                    {
                        "table_name": table_name,
                        "date_column": date_col,
                        "table_type": "Недели",
                        "exc_list": get_exceptions(get_col(row_data, 5)),
                    }
                )

            if is_checked(get_col(row_data, 6)):
                all_table_data.append(
                    {
                        "table_name": table_name,
                        "date_column": date_col,
                        "table_type": "Месяцы",
                        "exc_list": get_exceptions(get_col(row_data, 7)),
                    }
                )

    return all_table_data, table_names


def fetch_clickhouse_metadata(
    client: Client, table_names: List[str]
) -> Dict[str, Dict[str, Optional[str]]]:
    metadata: Dict[str, Dict[str, Optional[str]]] = {}
    if not table_names:
        return metadata

    try:
        sql = """
            SELECT
                table,
                argMax(database, data_compressed_bytes) as db_name,
                anyIf(name, lower(name) LIKE '%date%' AND lower(name) LIKE '%type%') as date_type_col
            FROM system.columns
            WHERE table IN {table_names:Array(String)}
            GROUP BY table
        """
        result = client.query_np(sql, parameters={"table_names": table_names})
        for row in result:
            metadata[row[0]] = {
                "database": row[1],
                "date_type_col": row[2] if row[2] else None,
            }
    except Exception as e:
        logging.error(
            "Failed to fetch bulk metadata from system.columns: %s\n%s",
            e,
            traceback.format_exc(),
        )

    return metadata


def process_tables(
    client: Client,
    all_table_data: List[Dict],
    metadata: Dict[str, Dict[str, Optional[str]]],
) -> None:
    message_count = 1
    for item in all_table_data:
        was_message_sent = check_table_gaps(
            client,
            item["table_name"],
            item["date_column"],
            item["exc_list"],
            item["table_type"],
            message_count,
            config.oauth_token,
            config.chat_id,
            config.dry_run,
            metadata,
        )
        if was_message_sent:
            message_count += 1


def main() -> None:
    setup_logging()

    client = get_clickhouse_client(
        host=config.host,
        username=config.ch_username,
        password=config.password,
        ca_cert=config.ca_cert,
    )

    try:
        range_values_map = fetch_all_table_configs(
            config.range_names, config.creds, config.spreadsheet_id
        )
    except Exception as e:
        logging.error(
            "Failed to fetch configuration from Google Sheets: %s\n%s",
            e,
            traceback.format_exc(),
        )
        return

    all_table_data, table_names = parse_sheet_data(range_values_map, config.range_names)
    metadata = fetch_clickhouse_metadata(client, table_names)
    process_tables(client, all_table_data, metadata)


if __name__ == "__main__":
    main()
