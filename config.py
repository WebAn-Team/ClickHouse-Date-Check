import json
import os
from dotenv import load_dotenv

load_dotenv()

is_ci = os.getenv("GITHUB_ACTIONS", "false").lower() == "true"

dry_run_env = os.getenv("DRY_RUN")
if dry_run_env is not None:
    dry_run = dry_run_env.strip().lower() in ("1", "true", "yes", "on")
else:
    dry_run = not is_ci

range_names_raw = os.getenv("range_names")
range_names = json.loads(range_names_raw) if range_names_raw else []

creds = os.getenv("creds", "")
spreadsheet_id = os.getenv("spreadsheet_id", "")
oauth_token = os.getenv("oauth_token", "")
chat_id = os.getenv("chat_id", "")
host = os.getenv("host", "")
ch_username = os.getenv("ch_username", "")
password = os.getenv("password", "")
ca_cert = os.getenv("ca_cert")
null_date = os.getenv("null_date", "1970-01-01")
ch_port = int(os.getenv("ch_port", "8443"))
ch_interface = os.getenv("ch_interface", "https")
