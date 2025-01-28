import json
import math
import os
import requests
from retry import retry
import pandas as pd
import datetime
from project.server.main.utils import get_raw_data_filename, get_transformed_data_filename, to_jsonl
from project.server.main.paysage import enrich_with_paysage
from project.server.main.monmaster import get_monmaster_elt
from project.server.main.sise import get_years_in_sise, get_sise_elt
from project.server.main.rncp import get_rncp_elt
from project.server.main.rome import get_rome_elt
from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object, download_object

logger = get_logger(__name__)

raw_data_suffix = '20250124'

def transform_raw_data(raw_data_suffix):
    raw_data_filename = get_raw_data_filename(raw_data_suffix)
    download_object('fresq', raw_data_filename, raw_data_filename)
    fresq_data = [e['data'] for e in pd.read_json(raw_data_filename).to_dict(orient='records')]
    fresq_enriched = enrich_with_paysage(fresq_data)
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
    assert(len(fresq_id)>5)
    uai_fresq = elt['uai_etablissement']
    # real PID is inf x UAI
    fresq_etab_id = f'{fresq_id}_{uai_fresq}'
    elt['fresq_etab_id'] = fresq_etab_id
    # cycle
    elt['cycle'] = get_cycle(elt['categorie_type_diplome'], elt['libelle_type_diplome'])
    # geoloc infos
    geolocalisations = []
    if 'geolocalisations' in elt and isinstance(elt['geolocalisations'], list):
        geolocalisations = elt['geolocalisations']
        if len(geolocalisations) > 1:
            logger.debug(f'multiple geoloc for {fresq_id}')
    for geolocalisation in geolocalisations:
        if geolocalisation.get('site_uai') and geolocalisation.get('site_uai') != uai_fresq:
            logger.debug(f"multiples UAI for inf {fresq_id} with uai_etablissement={uai_fresq} and in geoloc site_uai={geolocalisation.get('site_uai')}")
        if isinstance(geolocalisation, dict) and geolocalisation.get('site_geolocalisation', {}).get('coordinates'):
            geoloc_s = geolocalisation['site_geolocalisation']['coordinates'].replace('"', '').split(',')
            if '[' in geoloc_s[0] and ']' in geoloc_s[1]:
                longitude = float(geoloc_s[0].replace('[', ''))
                latitude = float(geoloc_s[1].replace(']', ''))
                geoloc = f"{elt['nom_etablissement']}###{latitude}###{longitude}"
                elt['geoloc'] = geoloc
    #monmaster
    monmaster_infos = get_monmaster_elt(fresq_id, uai_fresq)
    elt.update(monmaster_infos)

    #sise
    code_sise_fresq = str(elt['code_sise'])
    sise_infos = {}
    nb_has_sise_infos = 0
    elt['has_sise_infos_years'] = []
    annees = get_years_in_sise()
    for annee in annees:
        uai_to_use = uai_fresq # TODO better with previous UAIs
        sise_infos[annee] = get_sise_elt(uai_to_use, code_sise_fresq, annee)
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
    elt['sise_infos'] = sise_infos

    # rncp
    rncp_infos = get_rncp_elt(elt['num_rncp'])
    elt.update(rncp_infos)

    # rome
    rome_infos = get_rome_elt(elt['num_rncp'])
    elt.update(rome_infos)
    
    return elt
