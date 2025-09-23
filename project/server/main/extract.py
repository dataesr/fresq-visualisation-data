import json
import math
import os
import requests
from retry import retry
import pandas as pd
import datetime
import time

from project.server.main.utils import get_today, save_logs
from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object, download_object

logger = get_logger(__name__)

FRESQ_AUTHENT_URL = os.getenv('FRESQ_AUTHENT_URL')
FRESQ_LOGIN = os.getenv('FRESQ_LOGIN')
FRESQ_PASSWORD = os.getenv('FRESQ_PASSWORD')

FRESQ_BASE_URL = os.getenv('FRESQ_BASE_URL')
URL_DIPLOMES = f'{FRESQ_BASE_URL}/api/referentiels/type_diplome'
URL_SEARCH = f'{FRESQ_BASE_URL}/api/recherche'


API_SPECIFIC = {}
API_SPECIFIC['BUT'] = 'diplome_but'

@retry(delay=300, tries=3, logger=logger)
def get_headers():
    r = requests.post(FRESQ_AUTHENT_URL, data={
        'grant_type': 'password',
        'scope': 'openid',
        'username': FRESQ_LOGIN,
        'password': FRESQ_PASSWORD}, auth=('client-fresq', ''))
    tokens = r.json()
    key = tokens['access_token']
    headers= {'Authorization': f'Bearer {key}'}
    return headers

@retry(delay=300, tries=5, logger=logger)
def get_code_diplomes():
    type_diplomes = requests.get(URL_DIPLOMES, headers=get_headers()).json()['datas']
    code_diplomes = [t['data']['code'] for t in type_diplomes]
    return code_diplomes

def get_params(code_diplome, pageNumber):
    return {'uais': [],
     'codesTypeDiplome': [code_diplome],
     'term': '',
     'pageNumber': pageNumber,
     'pageSize': 100,
     'sortProperty': 'nom_etablissement_sort',
     'sortDirection': 'ASC',
     'searchInAttachments': False}

@retry(delay=300, tries=5, logger=logger)
def get_data(code_diplome):
    logger.debug(f'getting data from FRESQ for code_diplome {code_diplome}')
    data = []
    params = get_params(code_diplome, 0)
    current_headers = get_headers()
    r = requests.post(URL_SEARCH, headers=current_headers, json=params).json()
    time.sleep(1)
    nb_pages = r['totalPages']
    logger.debug(f'nb_pages = {nb_pages}')
    data = r['content']
    for p in range(1, nb_pages):
        print(p, end=',')
        params = get_params(code_diplome, p)
        r = requests.post(URL_SEARCH, headers=current_headers, json=params).json()
        #print(len(r['content']))
        data += r['content']
        time.sleep(1)
    logger.debug(f'{len(data)} elements retrieved for code {code_diplome}')
    assert(len(data) < 9999)
    return data

@retry(delay=300, tries=5, logger=logger)
def get_formation(technical_id, code_diplome):
    api_specific = ''
    if code_diplome in ['BUT']:
        api_specific = API_SPECIFIC[code_diplome]
    url = f'https://fresq.enseignementsup.gouv.fr/api/diplomes/{api_specific}/{technical_id}?stock=true'
    current_headers = get_headers()
    time.sleep(1)
    logger.debug(url)
    r = requests.get(url, headers=current_headers).json()
    formation = r['data']
    parcours = formation.get('parcours_diplomants', [])
    parcours_full = []
    # pour les parcours, malheureusement, un appel par parcours pour avoir le code sise
    if isinstance(parcours, list):
        for p in parcours:
            p_complet = get_parcours(p, code_diplome)
            parcours_full.append(p_complet)
    formation['parcours_diplomants_full'] = parcours_full
    # pour les étapes, un seul appel pour avoir la liste
    if isinstance(formation.get('etapes'), list) and len(formation['etapes'])>0:
        formation['etapes_details'] = get_etapes_list(technical_id, code_diplome)
    return formation

@retry(delay=300, tries=5, logger=logger)
def get_parcours(parcours_id, code_diplome):
    api_specific = ''
    if code_diplome in ['BUT']:
        api_specific = API_SPECIFIC[code_diplome]
    url = f'https://fresq.enseignementsup.gouv.fr/api/parcours-diplomants/{parcours_id}'
    current_headers = get_headers()
    time.sleep(1)
    r = requests.get(url, headers=current_headers).json()
    ans = r['data']
    return ans

@retry(delay=300, tries=5, logger=logger)
def get_etapes_list(technical_id, code_diplome):
    api_specific = ''
    if code_diplome in ['BUT']:
        api_specific = API_SPECIFIC[code_diplome]
    url = f'https://fresq.enseignementsup.gouv.fr/api/diplomes/{api_specific}/{technical_id}/etapes?pageSize=100'
    current_headers = get_headers()
    time.sleep(1)
    r = requests.get(url, headers=current_headers).json()
    ans = [e['data'] for e in r]
    return ans

def get_full_data():
    full_data = []
    code_diplomes = get_code_diplomes()
    for c in code_diplomes:
        new_data = get_data(c)
        full_data += new_data
    logger.debug(f'{len(full_data)} elements retrieved for all codes')
    for d in full_data:
        code_diplome = d['data']['code_type_diplome'] 
        if code_diplome in ['BUT']:
            d['data']['formation_details'] = get_formation(d['recordId'], code_diplome)
    return full_data


def save_data(data, suffix):
    current_file = f'fresq_raw_{suffix}.json'
    os.system(f'rm -rf {current_file}')
    json.dump(data, open(current_file, 'w'))
    os.system(f'rm -rf {current_file}.gz')
    os.system(f'gzip {current_file}')
    upload_object('fresq', f'{current_file}.gz', f'{current_file}.gz')
    os.system(f'rm -rf {current_file}.gz')

def extract_from_fresq():
    logger.debug('>>>>>>>>>> EXTRACT >>>>>>>>>>')
    full_data = get_full_data()
    current_date = get_today()
    save_data(full_data, current_date)
    save_data(full_data, 'latest')
    save_logs()
    return current_date
