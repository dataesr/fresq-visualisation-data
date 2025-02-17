import requests, json
import pandas as pd
import os
from project.server.main.utils import get_today
from project.server.main.utils_swift import upload_object, download_object

from project.server.main.logger import get_logger
logger = get_logger(__name__)

df_monmaster = None

def get_monmaster():
    logger.debug('>>>>> get MON MASTER >>>>>')
    headers = {'Accept': 'application/json'}
    params = {'size': '10000'}
    MONMASTER_URL = os.getenv('MONMASTER_URL')
    response = requests.post(MONMASTER_URL, json=params, headers=headers)
    #data = response.json()['hits']['hits']
    data = response.json()['content']
    logger.debug(f'{len(data)} records harvested from mon master')
    today = get_today()
    monmaster_filename = f'mon_master_{today}.json'
    json.dump(data, open(monmaster_filename, 'w'))
    upload_object('fresq', monmaster_filename, 'monmaster_latest.json')
    return monmaster_filename

def load_monmaster():
    monmaster_filename = ''
    try:
        monmaster_filename = get_monmaster()
    except:
        logger.debug(f'impossible to get data from monmaster - using last data available')
        download_object('fresq', 'monmaster_latest.json', 'monmaster_latest.json')
        monmaster_filename = 'monmaster_latest.json'
    #df_monmaster = pd.DataFrame(pd.read_json(monmaster_filename)['_source'].to_list())
    df_monmaster = pd.read_json(monmaster_filename)
    #df_monmaster = df_monmaster.set_index('idInm')
    df_monmaster = df_monmaster.set_index('inm')
    return df_monmaster

def get_monmaster_elt(inf, uai):
    global df_monmaster
    if df_monmaster is None:
        df_monmaster = load_monmaster()
    ans = {}
    has_monmaster_infos = False
    monmaster_elts = df_monmaster[(df_monmaster.index==inf) & (df_monmaster.uai==uai)].to_dict(orient='records')
    if(len(monmaster_elts)>1):
        logger.debug(f'{inf} {uai} apparait {len(monmaster_elts)} fois dans les données monmaster')
        logger.debug(f"data_quality;monmaster;multiple_monmaster_elts;{inf};{uai}")
    nb_masters = len(monmaster_elts)
    ans['nb_masters'] = nb_masters
    ans['masters'] = []
    for monmaster_elt in monmaster_elts:
        current_elt = {}
        # tous ces champs ont disparus
        for f in ['courses', 'keyWords', 'listSpecialityCourse', 'ifc', 'intituleMention', 'intituleParcours', 'inmp', 'commentaire', 'motifsLibres' ]:
            if monmaster_elt.get(f):
                current_elt[f] = monmaster_elt[f]
                has_monmaster_infos = True
        if current_elt:
            current_elt['inm'] = inf
            ans['masters'].append(current_elt)
    return {'monmaster_infos': ans, 'has_monmaster_infos': has_monmaster_infos}
