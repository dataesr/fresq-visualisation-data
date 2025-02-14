import json
import math
import os
import requests
from retry import retry
import pandas as pd
import datetime
import jsonlines
from project.server.main.utils import get_raw_data_filename, get_transformed_data_filename, to_jsonl, normalize, get_mentions_filename, get_etab_filename, get_df_fresq_raw
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
fresq_enriched = None

def transform_raw_data(raw_data_suffix='latest'):
    global fresq_enriched
    logger.debug('>>>>>>>>>> TRANSFORM >>>>>>>>>>')
    logger.debug(f'start fresq data from {raw_data_suffix} enrichment')
    global df_fresq_raw
    if df_fresq_raw is None:
        df_fresq_raw = get_df_fresq_raw(raw_data_suffix)
    fresq_data = []
    for e in df_fresq_raw.to_dict(orient='records'):
        new_elt = {}
        new_elt = e['data']
        for f in ['recordId', 'collectionId', 'bucketId']:
            if e.get(f):
                new_elt[f] = e.get(f)
        fresq_data.append(new_elt)
    fresq_data_with_paysage = [enrich_with_paysage(fresq_elt) for fresq_elt in fresq_data]
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
    #return fresq_enriched

def get_transformed_data(raw_data_suffix='latest'):
    transformed_data_filename = get_transformed_data_filename(raw_data_suffix)
    download_object('fresq', transformed_data_filename, transformed_data_filename)
    r = jsonlines.open(transformed_data_filename)
    return [elt for elt in r]

def get_mentions(raw_data_suffix='latest'):
    global fresq_enriched
    if fresq_enriched is None:
        fresq_enriched = get_transformed_data(raw_data_suffix)
    logger.debug('>>>>>>>>>> TRANSFORM MENTIONS >>>>>>>>>>')
    mentions_map = {}
    for e in fresq_enriched:
        if not isinstance(e.get('mention_id'), str):
            continue
        if e.get('mention_id') not in mentions_map:
            current_mention = {'formations': []}
            for f in ['mention_id', 'intitule_officiel', 'secteur', 'domaines',
                  'mots_cles', 'specialites',
                  'entityfishing_infos', 'has_entityfishing_infos',
                  'monmaster_infos', 'has_monmaster_infos',
                  'rncp_infos', 'has_rncp_infos',
                  'rome_infos', 'has_rome_infos',
                  'secteur_disciplinaire_sise',
                  'domaine_rattachement_1_cti', 'domaine_rattachement_2_cti','domaine_rattachement_autre_cti',
                  'libelle_formation_2', 'mention_normalized',
                  'libelle_type_diplome', 'code_sise',
                  'sise_secteur_disciplinaire', 'sise_discipline', 'sise_grande_discipline'
                 ]:
                if e.get(f):
                    current_mention[f] = e.get(f)
            mentions_map[e['mention_id']] = current_mention
        current_formation = {}
        for f in ['inf', 'fresq_etab_id', 'geoloc', 'mention_id',
                'nom_commun_etablissement', 'nom_etablissement', 'uai_etablissement', 'academie', 'paysage_id_to_use']:
            if e.get(f):
                current_formation[f] = e[f]
        mentions_map[e['mention_id']]['formations'].append(current_formation)
        mentions_map[e['mention_id']]['nb_formations'] = len(mentions_map[e['mention_id']]['formations'])
    x = list(mentions_map.values())
    logger.debug(f'{len(x)} mentions detected')
    df_mentions = pd.DataFrame(x)
    current_file = get_mentions_filename(raw_data_suffix)
    os.system(f'rm -rf {current_file}')
    to_jsonl(df_mentions.to_dict(orient='records'), current_file)
    upload_object('fresq', current_file, current_file)

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
    paysage_id_to_use = elt.get('paysage_id_to_use', '')
    if len(uai_fresq)<5:
        logger.debug(f"data_issue;bad UAI;{fresq_id};{uai_fresq}")
    if not isinstance(paysage_id_to_use, str) or len(paysage_id_to_use)<2:
        logger.debug(f"data_issue;no_paysage_id_found;{fresq_id};{uai_fresq}")
    # real PID is inf x UAI
    #fresq_etab_id = f'{fresq_id}_{uai_fresq}'
    fresq_etab_id = f'{fresq_id}_{paysage_id_to_use}'
    elt['fresq_etab_id'] = fresq_etab_id
    # mention
    mention_fresq = normalize(elt.get('intitule_officiel'), remove_space=False)
    mention_fresq = mention_fresq.replace('2nd degre', '2e degre')
    #mention_fresq = mention_fresq.replace('pratiques et ingenierie de la formation pif', 'pratiques et ingenierie de la formation')
    elt['mention_normalized'] = mention_fresq.title()
    elt['mention_id']=mention_fresq.replace(' ', '')
    # cycle
    elt['cycle'] = get_cycle(elt['categorie_type_diplome'], elt['libelle_type_diplome'])
    # geoloc infos - only sanity check
    # geoloc for main etab comes from paysage
    geolocalisations = []
    if 'geolocalisations' in elt and isinstance(elt['geolocalisations'], list):
        geolocalisations = elt['geolocalisations']
    for geolocalisation in geolocalisations:
        if isinstance(geolocalisation, dict) and geolocalisation.get('site_geolocalisation', {}).get('coordinates'):
            geoloc_s = geolocalisation['site_geolocalisation']['coordinates'].replace('"', '').split(',')
            if (len(geoloc_s) == 2) and ('[' in geoloc_s[0]) and (']' in geoloc_s[1]):
                longitude = float(geoloc_s[0].replace('[', ''))
                latitude = float(geoloc_s[1].replace(']', ''))
            else:
                logger.debug(f"data_issue;geoloc_ill_formed;{fresq_id};{uai_fresq};{geoloc_s}")
    #monmaster
    monmaster_infos = get_monmaster_elt(fresq_id, uai_fresq) # todo ? use paysage ?
    elt.update(monmaster_infos)
    
    #entity fishing
    ef_infos = get_entityfishing()
    elt.update(ef_infos)

    #sise
    list_code_sise_fresq = []
    if isinstance(elt.get('code_sise'), str):
        list_code_sise_fresq = [str(elt['code_sise'])]
    elif isinstance(elt.get('code_sise'), list):
        list_code_sise_fresq = elt['code_sise']
        for c in list_code_sise_fresq:
            assert(isinstance(c, str))
    else:
        assert(elt.get('code_sise') is None)
        logger.debug(f"data_issue;no_codeSISE;{fresq_id};{paysage_id_to_use}")
    sise_infos = {}
    nb_has_sise_infos = 0
    elt['has_sise_infos_years'] = []
    annees = get_years_in_sise()
    for annee in annees:
        #uai_to_use = uai_fresq # TODO better with previous UAIs
        sise_infos[annee] = get_sise_elt(paysage_id_to_use, list_code_sise_fresq, annee, fresq_id)
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
    if list_code_sise_fresq and (elt['nb_has_sise_infos'] == 0):
        logger.debug(f'code SISE {list_code_sise_fresq} absent from SISE data')
        logger.debug(f"data_issue;codeSISE_absent_from_SISE_data;{fresq_id};{paysage_id_to_use};{'-'.join(list_code_sise_fresq)}")

    num_rncp = None
    if isinstance(elt.get('num_rncp'), str) and 'RNCP' in elt['num_rncp']:
        num_rncp = elt['num_rncp']
    else:
        assert(elt.get('num_rncp') is None)
        logger.debug(f"data_issue;no_RNCP;{fresq_id};{paysage_id_to_use}")

    # rncp
    rncp_infos = get_rncp_elt(num_rncp)
    elt.update(rncp_infos)

    # rome
    rome_infos = get_rome_elt(num_rncp)
    elt.update(rome_infos)
    
    return elt
