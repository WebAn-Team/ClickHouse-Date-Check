import json
from typing import List, Dict

from google.oauth2 import service_account
from googleapiclient.discovery import build


def fetch_all_table_configs(
    sheets_ranges: List[str], creds: str, spreadsheet_id: str, timeout: int = 60
) -> Dict[str, List[List[str]]]:
    if not sheets_ranges:
        return {}

    credentials = service_account.Credentials.from_service_account_info(
        json.loads(creds),
        scopes=["https://www.googleapis.com/auth/spreadsheets.readonly"],
    )

    service = build("sheets", "v4", credentials=credentials, cache_discovery=False)

    result = (
        service.spreadsheets()
        .values()
        .batchGet(spreadsheetId=spreadsheet_id, ranges=sheets_ranges)
        .execute()
    )

    value_ranges = result.get("valueRanges", [])

    return {sheets_ranges[i]: vr.get("values", []) for i, vr in enumerate(value_ranges)}
