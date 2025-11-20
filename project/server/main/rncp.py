import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
import json
from project.server.main.logger import get_logger
from project.server.main.utils import download_file
logger = get_logger(__name__)

URL_RNCP_DATA_GOUV = 'https://www.data.gouv.fr/api/2/datasets/5eebbc067a14b6fecc9c9976/resources/?page=1&type=update&page_size=6&q'
df_rncp = None

def get_rncp():
    logger.debug('>>>>> get RNCP >>>>>')
    r = requests.get(URL_RNCP_DATA_GOUV).json()['data']
    for dataset in r:
        if 'export-fiches-rncp-v4-1' in dataset['title'] and 'csv' not in dataset['title']:
            break
    dataset_url = dataset['url']
    # dowload and upload to OVH
    rncp_zip_file = download_file(dataset_url, True)
    rncp_suffix = rncp_zip_file.replace('export-fiches-rncp-v4-1-', '').replace('.zip', '')
    os.system(f'unzip {rncp_zip_file}')
    rncp_filename = None
    for f in os.listdir():
        if ('RNCP' in f) and (rncp_suffix in f) and ('xml' in f):
            rncp_filename = f
    logger.debug(f'starting to parse {rncp_filename} ...')
    xml = open(rncp_filename, 'r').read()
    soup = BeautifulSoup(xml, 'xml')
    x = soup.find_all('FICHE')
    parsed = []
    for ix, fiche in enumerate(x):
        if ix%5000==0:
            logger.debug(f'parsing RNCP {ix}/{len(x)}')
        parsed.append(parse_fiche_rncp(fiche))
    rncp_parsed_filename = 'rncp_parsed_latest.json'
    json.dump(parsed, open(rncp_parsed_filename, 'w'))
    df_rncp = pd.DataFrame(pd.read_json(rncp_parsed_filename)).set_index('numero_fiche')
    logger.debug(f'rncp object created with {len(df_rncp)} elements')
    return df_rncp

def get_rncp_elt(num_rncps):
    ans = {'avec_rncp_infos': False, 'rncp_infos': {}}
    if not isinstance(num_rncps, list):
        return ans
    global df_rncp
    if df_rncp is None:
        df_rncp = get_rncp()
    df_tmp = df_rncp[df_rncp.index.isin(num_rncps)]#['type_emploi_accessibles']
    rncp_infos = []
    if len(df_tmp)>0:
        for elt in df_tmp.reset_index().to_dict(orient='records'):
            new_elt = {}
            new_elt['rncp'] = elt['numero_fiche']
            for f in ['type_emploi_accessibles']:
                if elt.get(f):
                    new_elt[f] = elt[f].strip()
            rncp_infos.append(new_elt)
        return {'avec_rncp_infos': True, 'rncp_infos': rncp_infos}
    return ans

def parse_fiche_rncp(e):
    elt = {}
    elt['label'] = get_value(e, 'INTITULE')
    for f in ['ID_FICHE', 'NUMERO_FICHE', 'ANCIENNE_CERTIFICATION', 'ETAT_FICHE', 
              'ACTIVITES_VISEES', 'REGLEMENTATIONS_ACTIVITES',
              'CAPACITES_ATTESTEES', 'SECTEURS_ACTIVITE', 'TYPE_EMPLOI_ACCESSIBLES', 'OBJECTIFS_CONTEXTE',
              'ACTIF', 'DATE_DE_PUBLICATION']:
        elt[f.lower()] = get_value(e, f)
    certificateurs = []
    if e.find('CERTIFICATEURS'):
        certificateurs_xml = e.find('CERTIFICATEURS').find_all('CERTIFICATEUR')
        for c in certificateurs_xml:
            certif = {}
            siret = get_value(c, 'SIRET_CERTIFICATEUR')
            label = get_value(c, 'NOM_CERTIFICATEUR')
            certif['siret'] = siret
            certif['label'] = label
            certificateurs.append(certif)
    elt['certificateurs'] = certificateurs
    elt['nb_certificateurs'] = len(certificateurs)

    partenaires = []
    if e.find('PARTENAIRES'):
        partenaires_xml = e.find('PARTENAIRES').find_all('PARTENAIRE')
        for p in partenaires_xml:
            part = {}
            siret = get_value(p, 'SIRET_PARTENAIRE')
            label = get_value(p, 'NOM_PARTENAIRE')
            habilitation = get_value(p, 'HABILITATION_PARTENAIRE')
            status_habilitation = get_value(p, 'ETAT_PARTENAIRE')
            start_habilitation = get_value(p, 'DATE_ACTIF')
            updated_habilitation = get_value(p, 'DATE_DERNIERE_MODIFICATION_ETAT')
            part['siret'] = siret
            part['label'] = label
            part['habilitation'] = {'label': habilitation, 
                                    'status': status_habilitation, 
                                    'start_date': start_habilitation,
                                     'last_status_update': updated_habilitation}
            partenaires.append(part)
        #elt['partenaires'] = partenaires
    elt['nb_partenaires'] = len(partenaires)
    
    elt['activites_visees'] = get_value(e, '')
    
    romes = []
    if e.find('CODES_ROME'):
        codes_rome = e.find('CODES_ROME').find_all('ROME')
        for c in codes_rome:
            code = get_value(c, 'CODE')
            label = get_value(c, 'LIBELLE')
            rome = {'code': code, 'label': label}
            romes.append(rome)
    elt['codes_rome'] = romes
    
    competences = []
    if e.find('BLOCS_COMPETENCES'):
        bloc_comp = e.find('BLOCS_COMPETENCES').find_all('BLOC_COMPETENCES')
        for b in bloc_comp:
            competence = {}
            for f in ['CODE', 'LISTE_COMPETENCES', 'MODALITES_EVALUATION']:
                competence[f.lower()] = get_value(b, f)
            competence['label'] = get_value(b, 'LIBELLE')
            competences.append(competence)
    elt['competences'] = competences
    
    return elt

def get_value(x, l):
    n = x.find(l)
    if n:
        return n.get_text()
    return None
