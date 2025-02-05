import requests
import pandas as pd
import os
import json

from project.server.main.logger import get_logger
from project.server.main.utils import download_file, to_jsonl, get_filename
from project.server.main.utils_swift import upload_object, download_object
logger = get_logger(__name__)

URL_CERTIF_DATA_GOUV = 'https://www.data.gouv.fr/api/2/datasets/60e2cb720550fe720cb7b1cc/resources/?page=1&type=main&page_size=6&q='
rome, rncp2rome = None, None

def get_rome():
    logger.debug('>>>>> get ROME >>>>>')
    #URL_ROME = 'https://www.francetravail.org/files/live/sites/peorg/files/documents/Statistiques-et-analyses/Open-data/ROME/ROME_ArboPrincipale.xlsx'
    #arbo_rome_file = download_file(URL_ROME, True)
    arbo_rome_file = 'ROME_ArboPrincipale.xlsx'
    df_rome = pd.read_excel(arbo_rome_file, sheet_name='Arbo Principale 14-06-2021')
    df_rome.columns = ['L', 'C1', 'C2', 'label', 'OGR']
    df_rome['ROME'] = df_rome.apply(lambda r: r['L']+str(r['C1'])+str(r['C2']), axis=1)

    level1, level2, level3, rome = {}, {}, {}, {}
    for e in df_rome.to_dict(orient='records'):
        if e['C1'] == ' ':
            level1[e['L']] = e['label']
        elif e['C2'] == ' ':
            level2[e['L']+str(e['C1'])] = e['label']
        elif len(e['OGR'].strip())==0:
            code_rome = e['L']+str(e['C1'])+str(e['C2'])
            level3[code_rome] = e['label']
        else:
            code_rome = e['L']+str(e['C1'])+str(e['C2'])
            if code_rome not in rome:
                rome[code_rome] = []
            rome[code_rome].append({
                'code_rome': code_rome,
               'id_level_1': e['L'],
                'level_1': level1[e['L']],
               'level_2': level2[e['L']+str(e['C1'])],
                'id_level_2': e['L']+str(e['C1']),
                'level_3': level3[code_rome],
                'label': e['label'],
                'ogr': str(e['OGR']).replace('.0', '')
            })
    json.dump(rome, open('rome.json', 'w'))
    logger.debug(f'rome object created with {len(rome)} elements')
    return rome

def get_rncp2rome():
    global rome
    if rome is None:
        rome = get_rome()
    r = requests.get(URL_CERTIF_DATA_GOUV).json()['data']
    for dataset in r:
        if ('opendata-certifinfo' in dataset['title']) and ('csv' in dataset['title']):
            break
    dataset_url = dataset['url']
    # dowload and upload to OVH
    # certifinfo_file = download_file(dataset_url, True) # does not work ??
    df_cf = pd.read_csv(dataset_url, sep=';', encoding='iso-8859-1')
    certifinfo_file = get_filename(dataset_url)
    df_cf.to_csv(certifinfo_file, index=False, sep=';')
    upload_object('fresq', certifinfo_file, certifinfo_file)
    rncp2rome = {}
    for e in df_cf[['Code_Diplome', 'Libelle_Diplome', 'Code_RNCP', 'Code_Ancien_RNCP', 'Code_Rome_1', 'Code_Rome_2', 'Code_Rome_3',
       'Code_Rome_4', 'Code_Rome_5']].to_dict(orient='records'):
        rncp = e['Code_RNCP']
        rncp_old = e['Code_Ancien_RNCP']
        for rncp_code in [rncp, rncp_old]:
            if rncp_code==rncp_code:
                rncp_code = 'RNCP'+str(int(rncp_code))
                for k in range(1, 6):
                    code_rome = e[f'Code_Rome_{k}']
                    if code_rome==code_rome:
                        if rncp_code not in rncp2rome:
                            rncp2rome[rncp_code] = []
                        if code_rome in rome:
                            rncp2rome[rncp_code] += rome[code_rome]
    json.dump(rncp2rome, open('rncp2rome.json', 'w'))
    logger.debug(f'rncp2rome oject created with {len(rncp2rome)} elements')
    get_metiers(rncp2rome)
    return rncp2rome

def get_metiers(rncp2rome):
    current_file = 'fresq_metiers.jsonl'
    logger.debug(f'computing referential metier file {current_file}')
    x = list(rncp2rome.values())
    metiers=[]
    for e in x:
        metiers+=e
    df_metiers = pd.DataFrame(metiers).drop_duplicates()
    df_final = df_metiers[['code_rome', 'id_level_1', 'level_1', 'id_level_2', 'level_2', 'level_3']].drop_duplicates()
    df_final.columns = ['code_rome', 'id_level_1', 'level_1', 'id_level_2', 'level_2', 'label']
    metiers_map = {}
    for i, row in df_metiers.iterrows():
        if row['code_rome'] not in metiers_map:
            metiers_map[row['code_rome']] = []
        metiers_map[row['code_rome']].append({'ogr': row['ogr'], 'label': row['label']})
    df_final['metiers'] = df_final['code_rome'].apply(lambda x:metiers_map[x])
    os.system(f'rm -rf {current_file}')
    to_jsonl(df_final.to_dict(orient='records'), current_file)

def get_rome_elt(num_rncp):
    ans = {'has_rome_infos': False, 'rome_infos': {}}
    if num_rncp is None:
        return ans
    global rncp2rome
    if rncp2rome is None:
        rncp2rome = get_rncp2rome()
    if num_rncp in rncp2rome:
        return {'has_rome_infos': True, 'rome_infos': rncp2rome[num_rncp]}
    return ans
