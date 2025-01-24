import json
import math
import os
import requests
from retry import retry
import pandas as pd
import datetime
from project.server.main.utils import get_raw_data_filename, get_transformed_data_filename, to_jsonl
from project.server.main.paysage import enrich_with_paysage

from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object, download_object

logger = get_logger(__name__)

def transform_raw_data(raw_data_suffix):
    raw_data_filename = get_raw_data_filename(raw_data_suffix)
    download_object('fresq', raw_data_filename, raw_data_filename)
    fresq_data = pd.read_json(raw_data_filename).to_dict(orient='records')
    fresq_enriched = enrich_with_paysage(fresq_data)
    transformed_data_filename = get_transformed_data_filename(raw_data_suffix)
    os.system(f'rm -rf {transformed_data_filename}')
    to_jsonl(fresq_enriched, transformed_data_filename)
    upload_object('fresq', transformed_data_filename, transformed_data_filename)
