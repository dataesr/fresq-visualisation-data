import json
import math
import os
import requests
from retry import retry
import pandas as pd
import datetime
from project.server.main.utils import get_raw_data_filename, get_transformed_data_filename, to_jsonl, normalize
from project.server.main.paysage import enrich_with_paysage
from project.server.main.monmaster import get_monmaster_elt
from project.server.main.sise import get_years_in_sise, get_sise_elt
from project.server.main.rncp import get_rncp_elt
from project.server.main.rome import get_rome_elt
from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object, download_object
from project.server.main.ef import get_entityfishing 

logger = get_logger(__name__)

raw_data_suffix = '20250124'

df_fresq_raw = None

def get_df_fresq_raw(raw_data_suffix):
    raw_data_filename = get_raw_data_filename(raw_data_suffix)
    download_object('fresq', raw_data_filename, raw_data_filename)
    return pd.read_json(raw_data_filename)

def transform_raw_data(raw_data_suffix):
    logger.debug('>>>>>>>>>> TRANSFORM >>>>>>>>>>')
    logger.debug(f'start fresq data from {raw_data_suffix} enrichment')
    global df_fresq_raw
    if df_fresq_raw is None:
        df_fresq_raw = get_df_fresq_raw(raw_data_suffix)
    fresq_data = [e['data'] for e in df_fresq_raw.to_dict(orient='records')]
    fresq_data_with_paysage = enrich_with_paysage(fresq_data)
    fresq_enriched = []
    for ix, e in enumerate(fresq_data_with_paysage):
        elt = enrich_fresq_elt(e)
        fresq_enriched.append(elt)
        if len(fresq_enriched) % 2000 == 0:
            logger.debug(f'{len(fresq_enriched)} / {len(fresq_data_with_paysage)}')
    transformed_data_filename = get_transformed_data_filename(raw_data_suffix)
    os.system(f'rm -rf {transformed_data_filename}')
    to_jsonl(fresq_enriched, transformed_data_filename)
    upload_object('fresq', transformed_data_filename, transformed_data_filename)

def get_cycle(cat, lib):
    for k in ['Master', '+ 5', '+ 4']:
        if k in lib:
            return 'M'
    for k in ['ingénieur', 'Santé']:
        if k in cat:
            return 'M'
    for k in ['Bachelor', 'Licence', 'Capacité', 'Diplôme Universitaire de Technologie', '+ 3', 'DAEU', 'DEUST']:
        if k in lib:
            return 'L'
    for k in ['Doctorat']:
        if k in lib:
            return 'D'
    return 'other'

def enrich_fresq_elt(elt):
    fresq_id = elt['inf']
    assert(isinstance(fresq_id, str))
    if(len(fresq_id)<5):
        logger.debug(f"data_issue;bad INF?;{fresq_id};")
    uai_fresq = elt.get('uai_etablissement', '')
    if len(uai_fresq)<5:
        logger.debug(f"data_issue;bad UAI;{fresq_id};{uai_fresq}")
    # real PID is inf x UAI
    fresq_etab_id = f'{fresq_id}_{uai_fresq}'
    elt['fresq_etab_id'] = fresq_etab_id
    # mention
    mention_fresq = normalize(elt.get('intitule_officiel'), remove_space=False)
    mention_fresq = mention_fresq.replace('2nd degre', '2e degre')
    #mention_fresq = mention_fresq.replace('pratiques et ingenierie de la formation pif', 'pratiques et ingenierie de la formation')
    elt['mention_normalized'] = mention_fresq.title()
    # cycle
    elt['cycle'] = get_cycle(elt['categorie_type_diplome'], elt['libelle_type_diplome'])
    # geoloc infos
    geolocalisations = []
    if 'geolocalisations' in elt and isinstance(elt['geolocalisations'], list):
        geolocalisations = elt['geolocalisations']
        if len(geolocalisations) > 1:
            logger.debug(f'multiple geoloc for {fresq_id}')
            logger.debug(f"data_issue;multiple_geoloc;{fresq_id};{uai_fresq}")
    for geolocalisation in geolocalisations:
        if geolocalisation.get('site_uai') and geolocalisation.get('site_uai') != uai_fresq:
            site_uai = geolocalisation.get('site_uai')
            logger.debug(f"multiples UAI for inf {fresq_id} with uai_etablissement={uai_fresq} and in geoloc site_uai={geolocalisation.get('site_uai')}")
            logger.debug(f"data_issue;multipleUAI_in_geoloc;{fresq_id};{uai_fresq}_{site_uai}")
        if isinstance(geolocalisation, dict) and geolocalisation.get('site_geolocalisation', {}).get('coordinates'):
            geoloc_s = geolocalisation['site_geolocalisation']['coordinates'].replace('"', '').split(',')
            if (len(geoloc_s) == 2) and ('[' in geoloc_s[0]) and (']' in geoloc_s[1]):
                longitude = float(geoloc_s[0].replace('[', ''))
                latitude = float(geoloc_s[1].replace(']', ''))
                geoloc = f"{elt['nom_etablissement']}###{latitude}###{longitude}"
                elt['geoloc'] = geoloc
            else:
                logger.debug(f"data_issue;geoloc_ill_formed;{fresq_id};{uai_fresq};{geoloc_s}")
    #monmaster
    monmaster_infos = get_monmaster_elt(fresq_id, uai_fresq)
    elt.update(monmaster_infos)
    
    #entity fishing
    ef_infos = get_entityfishing()
    elt.update(ef_infos)

    #sise
    code_sise_fresq = None
    if isinstance(elt.get('code_sise'), str):
        code_sise_fresq = str(elt['code_sise'])
    elif isinstance(elt.get('code_sise'), list):
        logger.debug(f'multiple SISE - what should we do?')
        logger.debug(f"data_issue;multiple_codeSISE;{fresq_id};{uai_fresq}")
    else:
        assert(elt.get('code_sise') is None)
        logger.debug(f"data_issue;no_codeSISE;{fresq_id};{uai_fresq}")
    sise_infos = {}
    nb_has_sise_infos = 0
    elt['has_sise_infos_years'] = []
    annees = get_years_in_sise()
    for annee in annees:
        uai_to_use = uai_fresq # TODO better with previous UAIs
        sise_infos[annee] = get_sise_elt(uai_to_use, code_sise_fresq, annee, fresq_id)
        if sise_infos[annee].get('has_sise_infos'):
            nb_has_sise_infos += 1
            elt['has_sise_infos_years'].append(annee)
        for f in ['code_sise_found', 'sise_discipline', 'sise_secteur_disciplinaire', 'sise_grande_discipline']:
            if sise_infos[annee].get(f):
                elt[f] = sise_infos[annee][f]
        if annee == annees[-1]:
            for f in ['sise_matching']:
                if sise_infos[annee].get(f):
                    elt[f] = sise_infos[annee][f]
            elt['has_sise_infos_last'] = sise_infos[annee]['has_sise_infos']
        elt['nb_has_sise_infos'] = nb_has_sise_infos
        # simplify data - remove some fields
        for k in sise_infos[annee]['sise_infos']:
            for f in ['grande_discipline_code', 'grande_discipline', 
                  'secteur_disciplinaire_code', 'secteur_disciplinaire',
                 'discipline_code', 'discipline', 'annee_universitaire']:
                if k.get(f):
                    del k[f]
            if k.get('implantation_code_commune'):
                k['implantation_code_commune'] = str(k['implantation_code_commune'])
        for f in ['sise_discipline', 'sise_grande_discipline', 'sise_secteur_disciplinaire']:
            if sise_infos[annee].get(f):
                del sise_infos[annee][f]
    elt['sise_infos'] = sise_infos
    if code_sise_fresq and (elt['nb_has_sise_infos'] == 0):
        logger.debug(f'code SISE {code_sise_fresq} absent from SISE data')
        logger.debug(f"data_issue;codeSISE_absent_from_SISE_data;{fresq_id};{uai_fresq};{code_sise_fresq}")

    num_rncp = None
    if isinstance(elt.get('num_rncp'), str) and 'RNCP' in elt['num_rncp']:
        num_rncp = elt['num_rncp']
    else:
        assert(elt.get('num_rncp') is None)
        logger.debug(f"data_issue;no_RNCP;{fresq_id};{uai_fresq}")

    # rncp
    rncp_infos = get_rncp_elt(num_rncp)
    elt.update(rncp_infos)

    # rome
    rome_infos = get_rome_elt(num_rncp)
    elt.update(rome_infos)
    
    return elt
