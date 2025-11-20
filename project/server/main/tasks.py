import time
import datetime
import os
import requests
from project.server.main.extract import extract_from_fresq
from project.server.main.paysage import get_etabs
from project.server.main.transform import transform_raw_data, get_mentions
from project.server.main.load import load_fresq
from project.server.main.utils import get_today
from project.server.main.elastic import update_all_aliases
from project.server.main.logger import get_logger

logger = get_logger(__name__)

def create_dump_fresq():
    raw_data_suffix = extract_from_fresq()

def create_task_fresq(arg):
    extract = arg.get('extract', True)
    transform = arg.get('transform', True)
    load = arg.get('load', True)
    change_alias = arg.get('change_alias', False)

    today = get_today()
    index_name = arg.get('index_name', f'fresq-{today}')

    if index_name is None:
        logger.debug('missing index name')
        return

    raw_data_suffix = arg.get('raw_data_suffix', 'latest')
    #if extract is False and raw_data_suffix is None:
    #    logger.debug('Extract bool is false, so raw data should be retrieved but raw_data_suffix is missing')
    #    return
    
    if extract:
        _ = extract_from_fresq()

    if transform:
        # etabs
        get_etabs(raw_data_suffix)
        transform_raw_data(raw_data_suffix)
        # mentions
        get_mentions(raw_data_suffix)

    if load:
        load_fresq(raw_data_suffix, index_name)

    if change_alias:
        dated_suffix = index_name.replace('fresq-', '')
        year = dated_suffix[0:4]
        update_all_aliases(dated_suffix, f'{year}-staging')
