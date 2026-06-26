from typing import Any, Dict, List, Optional, Tuple

import clickhouse_connect
from clickhouse_connect.driver.client import Client
import config

NULL_DATE = config.null_date  # Для фильтра от нулевых дат


def get_clickhouse_client(
    host: str, username: str, password: str, ca_cert: Optional[str]
) -> Client:
    return clickhouse_connect.get_client(
        host=host,
        port=config.ch_port,
        username=username,
        password=password,
        interface=config.ch_interface,
        connect_timeout=10,
        send_receive_timeout=300,
        **({"ca_cert": ca_cert} if ca_cert else {}),
    )


def build_gap_query(
    table: str,
    date_column: str,
    date_type_col: Optional[str],
    table_type: str,
    exc_list: List[str],
) -> Tuple[str, Dict[str, Any]]:
    quoted_table = ".".join(f"`{part}`" for part in table.split("."))
    quoted_date_column = f"`{date_column}`"

    parameters: Dict[str, Any] = {"null_date": NULL_DATE}

    if table_type == "Месяцы":
        interval = "- INTERVAL 1 MONTH"
        array_join_expr = "DISTINCT toStartOfMonth(arrayJoin(arrayMap(x -> toDate(x), range(toUInt32(ifNull(start, end)), toUInt32(end), 24 * 3600))))"
        date_type_vals = ["По месяцам", "MAU"]
    elif table_type == "Недели":
        interval = "- INTERVAL 1 WEEK"
        array_join_expr = "DISTINCT toStartOfWeek(arrayJoin(arrayMap(x -> toDate(x), range(toUInt32(ifNull(start, end)), toUInt32(end), 24 * 3600))), 1)"
        date_type_vals = ["По неделям", "WAU"]
    else:
        interval = ""
        array_join_expr = "arrayJoin(arrayMap(x -> toDate(x), range(toUInt32(ifNull(start, end)), toUInt32(end), 24 * 3600)))"
        date_type_vals = ["По дням", "DAU"]

    date_type_condition = ""
    if date_type_col:
        date_type_condition = (
            f"AND `{date_type_col}` IN {{date_type_vals:Array(String)}}"
        )
        parameters["date_type_vals"] = date_type_vals

    exception_condition = ""
    if exc_list:
        exception_condition = "AND toString(dates) NOT IN {exceptions:Array(String)}"
        parameters["exceptions"] = exc_list

    sql = f"""
        WITH
            (SELECT toStartOfDay(toDate(MIN({quoted_date_column}))) FROM {quoted_table} WHERE toString({quoted_date_column}) <> {{null_date:String}} {date_type_condition}) AS start,
            toStartOfDay(now()) {interval} AS end
        SELECT {array_join_expr} dates
        WHERE dates NOT IN
            (SELECT DISTINCT toDate({quoted_date_column})
            FROM {quoted_table})
            {exception_condition}"""

    return sql, parameters
