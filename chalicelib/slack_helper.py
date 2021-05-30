from slack import WebClient
import os

class Slack:
    def __init__(self):
        self.client = WebClient(token=os.environ["slack_token"])

    def send_msg(self, text, channel):
        response = self.client.chat_postMessage(channel=channel,
                                                text=text)
        return response