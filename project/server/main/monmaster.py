import requests, json
import os
from project.server.main.utils import get_today
from project.server.main.utils_swift import upload_object, download_object

from project.server.main.logger import get_logger
logger = get_logger(__name__)

def get_monmaster():
    headers = {'Accept': 'application/json'}
    params = {'size': '10000'}
    MONMASTER_URL = os.getenv('MONMASTER_URL')
    response = requests.post(MONMASTER_URL, json=params, headers=headers)
    data = response.json()['hits']['hits']
    logger.debug(f'{len(data)} records harvested from mon master')
    today = get_today()
    monmaster_filename = f'mon_master_{today}.json'
    json.dump(data, open(monmaster_filename, 'w'))
    upload_object('fresq', monmaster_filename, 'monmaster_latest.json')
    return monmaster_filename
