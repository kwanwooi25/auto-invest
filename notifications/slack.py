# -*- coding: utf-8 -*-

import requests
from dotenv import load_dotenv
import os

load_dotenv()

URL = os.getenv('SLACK_WEBHOOK_URL')

def SendMessage(msg, title='자동매매 알림'):
    try:
        # 메시지 전송
        requests.post(
            URL,
            headers={
                'content-type': 'application/json'
            },
            json={
                'text': title,
                'blocks': [
                    {
                        'type': 'section',
                        'text': {
                            'type': 'mrkdwn',
                            'text': msg
                        }
                    }
                ]
            }
        )
    except Exception as ex:
        print(ex)
