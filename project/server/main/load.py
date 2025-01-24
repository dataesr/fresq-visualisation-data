import json
import math
import os
import requests
from retry import retry
from urllib import parse

from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object, download_object
from project.server.main.utils import get_transformed_data_filename
from project.server.main.elastic import reset_index_fresq, refresh_index

logger = get_logger(__name__)

ES_URL = os.getenv('ES_URL')
ES_LOGIN_FRESQ_BACK = os.getenv('ES_LOGIN_FRESQ_BACK')
ES_PASSWORD_FRESQ_BACK = os.getenv('ES_PASSWORD_FRESQ_BACK')

def load(raw_data_suffix, index_name):
    transformed_data_filename = get_transformed_data_filename(raw_data_suffix)
    download_object('fresq', transformed_data_filename, transformed_data_filename)
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_FRESQ_BACK}:{parse.quote(ES_PASSWORD_FRESQ_BACK)}@{es_url_without_http}'
    reset_index_fresq(index=index_name)
    elasticimport = f"elasticdump --input={transformed_data_filename} --output={es_host}{index_name} --type=data --limit 1000 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
    #os.system(elasticimport)
    refresh_index(index_name)
