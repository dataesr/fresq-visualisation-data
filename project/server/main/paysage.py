import requests
import retry
import os
import pickle
import pandas as pd
from project.server.main.utils import get_df_fresq_raw, get_etab_filename, to_jsonl
from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger
logger = get_logger(__name__)

API_KEY = os.getenv('PAYSAGE_API_KEY')
PAYSAGE_URL = os.getenv('PAYSAGE_URL')

headers = {
    'Content-Type': 'application/json',
    'x-api-key': API_KEY
}

paysage_uai_map, final_uai_paysage_correspondance = {}, {}

def enrich_with_paysage(elt):
    global final_uai_paysage_correspondance
    if len(final_uai_paysage_correspondance)==0:
        download_object('fresq', 'final_uai_paysage_correspondance.pkl', 'final_uai_paysage_correspondance.pkl')
        final_uai_paysage_correspondance = pickle.load(open('final_uai_paysage_correspondance.pkl', 'rb'))
    uai = elt.get('uai_etablissement')
    if not isinstance(uai, str):
        logger.debug(f"error;noUAI;{elt['inf']}")
        return elt
    if uai not in final_uai_paysage_correspondance:
        logger.debug(f'{uai} not in final_uai_paysage_correspondance ??')
        return elt
    paysage_infos = final_uai_paysage_correspondance[uai]
    elt.update(paysage_infos)
    return elt

def get_etabs(raw_data_suffix):
    global paysage_uai_map
    global final_uai_paysage_correspondance
    df_raw = get_df_fresq_raw(raw_data_suffix)
    data = df_raw.data.to_list()
    data_etab = []
    for d in data:
        etab_elt = {}
        for f in d:
            if '_etablissement' in f and isinstance(d[f], str):
                etab_elt[f] = d[f]
        data_etab.append(etab_elt)
    df_etabs = pd.DataFrame(data_etab).drop_duplicates()
    uais = list(set(df_etabs.uai_etablissement.to_list()))
    assert(len(uais) == len(df_etabs))
    logger.debug(f'Number UAI (main) found = {len(uais)}')
    #tmp = []
    #for d in data:
    #    if d.get('geolocalisations'):
    #        for g in d['geolocalisations']:
    #            for f in g:
    #                if 'uai' in f:
    #                    if g[f] not in uais:
    #                        tmp.append(g)
    #df_uais_in_geoloc = pd.DataFrame(tmp)
    #del df_uais_in_geoloc['site_geolocalisation']
    #del df_uais_in_geoloc['id']
    #df_uais_in_geoloc = df_uais_in_geoloc.drop_duplicates()
    #uais2 = df_uais_in_geoloc.site_uai.to_list()
    #logger.debug(f'Extra UAI found in geoloc = {len(uais2)}')
    
    paysage_uai_map = {}
    for uai in uais:
        get_paysage_search(uai)
    
    new_data = []
    for d in data_etab:
        uai = d.get('uai_etablissement')
        if not isinstance(uai, str):
            continue
        x = get_paysage_search(uai)
        paysage_elt = x['data'] #actif
        parent_elt = x['parents']
        successeur_elt = x['successeurs']
        paysage_nb = len(paysage_elt)
        paysage_id, paysage_id_to_use, geoloc = None, None, None
        uai_to_paysage_method = 'no'
        if len(paysage_elt) == 1:
            paysage_id = paysage_elt[0]['id']
            geoloc = get_geoloc_infos(paysage_elt[0])
            paysage_id_to_use = paysage_id
            uai_to_paysage_method = 'direct'
        if parent_elt:
            paysage_id_to_use = parent_elt[0]['id']
            geoloc = get_geoloc_infos(parent_elt[0])
            d.update(geoloc)
            uai_to_paysage_method = 'parent'
        if paysage_id_to_use is None and successeur_elt:
            paysage_id_to_use = successeur_elt[0]['id']
            uai_to_paysage_method = 'successeur'
            geoloc = get_geoloc_infos(successeur_elt[0])
            d.update(geoloc)
        d['paysage_id'] = paysage_id
        d['paysage_id_to_use'] = paysage_id_to_use
        d['uai_to_paysage_method'] = uai_to_paysage_method
        final_uai_paysage_correspondance[uai] = d
        new_data.append(d)
    df_new = pd.DataFrame(new_data).drop_duplicates()
    current_file = get_etab_filename(raw_data_suffix)
    logger.debug(f'{len(new_data)} rows for etabs')
    os.system(f'rm -rf {current_file}')
    to_jsonl(df_new.to_dict(orient='records'), current_file)
    upload_object('fresq', current_file, current_file)
    pickle.dump(final_uai_paysage_correspondance, open('final_uai_paysage_correspondance.pkl', 'wb'))
    upload_object('fresq', 'final_uai_paysage_correspondance.pkl', 'final_uai_paysage_correspondance.pkl')

def get_geoloc_infos(paysage_elt):
    new = {}
    geoloc = None
    name = paysage_elt['name']
    new['paysage_name'] = name
    if isinstance(paysage_elt.get('coordinates'), list):
        lat = paysage_elt['coordinates'][1]
        lon = paysage_elt['coordinates'][0]
        geoloc = f'{name}###{lat}###{lon}'
        new['geoloc'] = geoloc
    return new

#@retry.retry(tries=5, delay=4)
def get_paysage(paysage_id):
    #print(paysage_id)
    url=f'{PAYSAGE_URL}/autocomplete?query={paysage_id}&limit=50&types=structures'
    response = requests.get(url, headers=headers).json()
    assert (len(response['data'])==1)
    return response['data'][0]

#@retry.retry(tries=5, delay=4)
def get_paysage_search(uai):
    global paysage_uai_map
    if uai in paysage_uai_map:
        return paysage_uai_map[uai]
    url=f'{PAYSAGE_URL}/autocomplete?query={uai}&limit=50&types=structures'
    response = requests.get(url, headers=headers).json()
    data, data_active = [], []
    for d in response['data']:
        if uai in d.get('identifiers') and d.get('isDeleted') is False:
            data.append(d)
            if d.get('structureStatus')=='active':
                data_active.append(d)
    if(len(data_active) > 1):
        logger.debug(f'pb uai {uai}')
        assert(len(data_active)==1)
    parents = []
    successeurs = []
    if data_active:
        parents = get_paysage_parents(data[0]['id'])
    if len(data_active) == 0 and data:
        successeurs = get_paysage_successeurs(data[0]['id'])
    ans = {'data': data_active, 'parents':parents, 'successeurs': successeurs}
    paysage_uai_map[uai] = ans
    return ans

def get_paysage_parents(paysage_id):
    url = f"{PAYSAGE_URL}/relations?filters[relationTag]=structure-interne&filters[relatedObjectId]={paysage_id}"
    response_data = requests.get(url, headers=headers).json()['data']
    actives = []
    for d in response_data:
        if d.get('endDate') is None and (d.get('active') is not False):
            actives.append(d)
    if(len(actives) > 1):
        print(f'more than 1 parent for {paysage_id}')
        return (actives)
    parents = [get_paysage(k['resourceId']) for k in actives]
    return parents

def get_paysage_successeurs(paysage_id):
    url = f"{PAYSAGE_URL}/relations?filters[relationTag]=structure-predecesseur&filters[relatedObjectId]={paysage_id}&sort=-startDate"
    response_data = requests.get(url, headers=headers).json()['data']
    successeurs = [get_paysage(k['resourceId']) for k in response_data]
    return successeurs
