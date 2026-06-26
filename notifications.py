import logging
import requests


def send_yandex_message(
    oauth_token: str, chat_id: str, message: str, dry_run: bool = False
) -> None:
    if dry_run:
        logging.info("[DRY RUN - no Yandex Messenger] %s", message)
        return
    api_url = "https://botapi.messenger.yandex.net/bot/v1/messages/sendText/"
    headers = {
        "Authorization": f"OAuth {oauth_token}",
        "Content-Type": "application/json",
    }
    payload = {"chat_id": chat_id, "text": message}
    response = requests.post(api_url, json=payload, headers=headers, timeout=10)
    response.raise_for_status()
