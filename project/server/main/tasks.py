import time
import datetime
import os
import requests
from project.server.main.extract import extract_from_fresq
from project.server.main.transform import transform_raw_data
from project.server.main.load import load_fresq

from project.server.main.logger import get_logger

logger = get_logger(__name__)

def create_dump_fresq():
    raw_data_suffix = extract_from_fresq()

def create_task_fresq(arg):
    extract = arg.get('extract', True)
    transform = arg.get('transform', True)
    index_name = arg.get('index_name')

    if index_name is None:
        logger.debug('missing index name')
        return

    raw_data_suffix = arg.get('raw_data_suffix')
    if extract is False and raw_data_suffix is None:
        logger.debug('Extract bool is false, so raw data should be retrieved but raw_data_suffix is missing')
        return
    
    if extract:
        raw_data_suffix = extract_from_fresq()
    assert(isinstance(raw_data_suffix, str))

    if transform:
        transform_raw_data(raw_data_suffix)
        # mentions
        get_mentions(raw_data_suffix)
        # etabs
        get_etabs(raw_data_suffix)

    if load:
        load(raw_data_suffix, index_name)
