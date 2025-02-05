import json
import math
import os
import requests
from retry import retry

from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object, download_object
from project.server.main.utils import get_transformed_data_filename, get_mentions_filename, get_etab_filename
from project.server.main.elastic import reset_index, refresh_index, get_es_host, get_mappings_fresq, get_mappings_mentions, get_mappings_etab, get_mappings_metiers

logger = get_logger(__name__)

def load_metiers(index_name='fresq-metiers'):
    logger.debug('>>>>>>>>>> LOAD METIERS >>>>>>>>>>')
    current_file = 'fresq_metiers.jsonl'
    mappings_metiers = get_mappings_metiers()
    reset_index(index=index_name, mappings = mappings_metiers)
    es_host = get_es_host()
    elasticimport = f"elasticdump --input={current_file} --output={es_host}{index_name} --type=data --limit 1000 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
    os.system(elasticimport)
    refresh_index(index_name)

def load_etabs(raw_data_suffix, index_name='fresq-etablissements-2'):
    logger.debug('>>>>>>>>>> LOAD ETABS >>>>>>>>>>')
    etab_filename = get_etab_filename(raw_data_suffix)
    download_object('fresq', get_etab_filename, get_etab_filename)
    mappings_etab = get_mappings_etab()
    reset_index(index=index_name, mappings = mappings_etab)
    es_host = get_es_host()
    elasticimport = f"elasticdump --input={etab_filename} --output={es_host}{index_name} --type=data --limit 1000 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
    os.system(elasticimport)
    refresh_index(index_name)

def load_mentions(raw_data_suffix, index_name='fresq-mentions'):
    logger.debug('>>>>>>>>>> LOAD MENTIONS >>>>>>>>>>')
    mentions_filename = get_mentions_filename(raw_data_suffix)
    download_object('fresq', get_mentions_filename, get_mentions_filename)
    mappings_mentions = get_mappings_mentions()
    reset_index(index=index_name, mappings = mappings_mentions)
    es_host = get_es_host()
    elasticimport = f"elasticdump --input={mentions_filename} --output={es_host}{index_name} --type=data --limit 1000 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
    os.system(elasticimport)
    refresh_index(index_name)

def load_fresq(raw_data_suffix, index_name):
    logger.debug('>>>>>>>>>> LOAD FRESQ >>>>>>>>>>')
    load_metiers('fresq-metiers')
    load_mentions(raw_data_suffix, 'fresq-mentions')
    load_etabs(raw_data_suffix, 'fresq-etablissements-2')
    transformed_data_filename = get_transformed_data_filename(raw_data_suffix)
    download_object('fresq', transformed_data_filename, transformed_data_filename)
    mappings_fresq = get_mappings_fresq()
    reset_index(index=index_name, mappings = mappings_fresq)
    es_host = get_es_host()
    elasticimport = f"elasticdump --input={transformed_data_filename} --output={es_host}{index_name} --type=data --limit 1000 --noRefresh " + "--transform='doc._source=Object.assign({},doc)'"
    os.system(elasticimport)
    refresh_index(index_name)
