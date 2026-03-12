import os

import requests
from requests.auth import HTTPBasicAuth

DISK_THRESHOLD = 75

MM_CHANNEL="ticket-office-assistant"
BOT_ICON_URL="https://img.icons8.com/emoji/48/000000/robot-emoji.png"
ES_USER_AD = os.getenv('ES_USER_AD')
ES_PASSWORD_AD = os.getenv('ES_PASSWORD_AD')
ES_URL = os.getenv('ES_URL')
MATTERMOST_WEBHOOK_URL = os.getenv('MATTERMOST_WEBHOOK_URL')

def get_nodes_stats():
    url = f"{ES_URL}_cat/allocation?v"

    response = requests.get(
        url,
        auth=HTTPBasicAuth(ES_USER_AD, ES_PASSWORD_AD),
        verify=False
    )
    response.raise_for_status()
    return response.text

def send_mattermost_alert(stats):
    lines = ["### :warning: Alerte Elasticsearch — Disque > {}%".format(DISK_THRESHOLD), ""]
    lines.append(stats)
    payload = {
        "channel":MM_CHANNEL,
        "icon_url": BOT_ICON_URL,
        "username": "Elastic - Disk alert",
        "text": "\n".join(lines)
    }
    response = requests.post(MATTERMOST_WEBHOOK_URL, json=payload)

    response.raise_for_status()
    print(f"Alerte Mattermost envoyée.")
    
def check_disk():
    alert = False
    stats = get_nodes_stats()
    fields = [k for k in stats.splitlines()[0].split(' ') if k]
    vals = {}
    for i in range(1,4):
        current_vals = [k for k in stats.splitlines()[i].split(' ') if k]
        for fx, f in enumerate(fields):
            if f not in vals:
                vals[f]=[]
            vals[f].append(current_vals[fx])
            if f == 'disk.percent':
                if float(current_vals[fx])>DISK_THRESHOLD:
                    alert = True
    if alert:
        send_mattermost_alert(stats)
        
check_disk()
