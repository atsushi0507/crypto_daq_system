import os
import requests
from datetime import datetime, timezone # トリガー判定用に一時的に
import pytz


class Notifier:
    def __init__(self, channel_token: str, user_id: str):
        self.channel_token = channel_token
        self.user_id = user_id

    def send_line_push(self, message: str, image_url: str = None):
        url = "https://api.line.me/v2/bot/message/push"
        headers = {
            "Authorization": f"Bearer {self.channel_token}",
            "Content-Type": "application/json"
        }
        msg_list = [{"type": "text", "text": message}]
        if image_url:
            msg_list.append({
                "type": "image",
                "originalContentUrl": image_url,
                "previewImageUrl": image_url
            })

        data = {"to": self.user_id, "messages": msg_list}
        resp = requests.post(url, headers=headers, json=data)
        if resp.status_code != 200:
            print("LINE API error: ", resp.status_code, resp.text)
        else:
            print("LINE push sent!")

    def check_trigger(self, df, notify_config):
        jst = pytz.timezone("Asia/Tokyo")
        now = datetime.now(jst)
        hour, minute = now.hour, now.minute

        if (hour, minute) in [(9, 00), (18, 0)]:
            return True
        
        return False
