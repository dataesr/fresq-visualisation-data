import json
import math
import os
import requests
from retry import retry
import pandas as pd
import datetime
import jsonlines
from project.server.main.utils import get_raw_data_filename, get_transformed_data_filename, to_jsonl, normalize, get_mentions_filename, get_etab_filename, get_df_fresq_raw, save_logs
from project.server.main.paysage import enrich_with_paysage
from project.server.main.monmaster import get_monmaster_elt
from project.server.main.sise import get_years_in_sise, get_sise_elt, get_clean_sise_code_as_list
from project.server.main.rncp import get_rncp_elt
from project.server.main.rome import get_rome_elt
from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object, download_object
from project.server.main.ef import get_entityfishing 

logger = get_logger(__name__)

raw_data_suffix = 'latest'

df_fresq_raw = None
fresq_enriched = None

def group_by_inf(raw_data):
    logger.debug(f'raw fresq_data len = {len(raw_data)}')
    inf_dict = {}
    for e in raw_data:
        d = e['data']
        for f in ['recordId', 'collectionId', 'bucketId']:
            if e.get(f):
                d[f] = e.get(f)
        current_inf = d['inf']
        if current_inf not in inf_dict:
            inf_dict[current_inf] = {}
        if 'uai_etablissement' in d:
            uai = d.get('uai_etablissement')
        else:
            nb_elts = len(inf_dict[current_inf])
            uai = f'uai_absent_{nb_elts}'
            #logger.debug(f'pas de UAI pour inf {current_inf} - {uai}')
        inf_dict[current_inf][uai] = d
    inf_data = []
    for inf in inf_dict:
        inf_data.append(merge(inf, inf_dict[inf]))
    logger.debug(f'after INF group by fresq_data len = {len(inf_data)}')
    return inf_data

def merge(inf, list_elts):
    assert(len(list_elts)>0)
    ans = {'inf': inf}
    etablissements = []
    uais_etablissements = []
    first_uai = list(list_elts.keys())[0]
    for uai in list_elts:
        uais_etablissements.append(uai)
        for f in ['inf', 'categorie_type_diplome', 'libelle_type_diplome', 'intitule_officiel', 'mention_normalized', 'mention_normalized', 'mention_id', 'cycle',
            'code_sise_valid', 'code_sise_found', 'nb_code_sise_found', 'code_sise_invalid', 'code_sise', 'code_type_diplome', 'ordre_type_diplome',
            'date_debut_accreditation_min', 'date_fin_accreditation_max', 'domaines', 'identifiant_source',
            'recordId', 'collectionId', 'bucketId',
            'num_rncp', 'formation_details', 'sigle_sante', 'sigle_specialite_but', 'nom_specialite_but', 'specialite_sante', 'specialites', 'specialites_cti',
            'type_parcours_but', 'secteur_disciplinaire_sise']:
            if f in list_elts[uai]:
                try:
                    assert(list_elts[uai][f] == list_elts[first_uai][f])
                except:
                    logger.debug(f'assertion error for {uai} {f} as {list_elts[uai][f]} DIFFERENT FROM {list_elts[first_uai][f]}')
                ans[f] = list_elts[first_uai][f]
        current_etab = {}
        for f in list_elts[uai].keys():
            # champs qui contiennent ...
            for g in ['vague', 'type_delivrance', 'etabli', 'academi', 'tutelle', 'geoloc']:
                if g in f:
                    current_etab[f] = list_elts[uai][f]
            # champs qui vaut exactement ...
            for g in ['secteur']:
                if g == f:
                    current_etab[f] = list_elts[uai][f]
            if f == 'geolocalisations': # for dedup
                new_geoloc = []
                for geoloc in current_etab[f]:
                    if geoloc not in new_geoloc:
                        new_geoloc.append(geoloc)
                current_etab[f] = new_geoloc
        etablissements.append(current_etab)
    ans['etablissements'] = etablissements
    ans['nb_etablissements'] = len(etablissements)
    ans['uais_etablissements'] = uais_etablissements
    return ans

def transform_raw_data(raw_data_suffix='latest'):
    global fresq_enriched
    logger.debug('>>>>>>>>>> TRANSFORM >>>>>>>>>>')
    logger.debug(f'start fresq data from {raw_data_suffix} enrichment')
    global df_fresq_raw
    if df_fresq_raw is None:
        df_fresq_raw = get_df_fresq_raw(raw_data_suffix)
    raw_data = df_fresq_raw.to_dict(orient='records')
    fresq_data = group_by_inf(raw_data)
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
    save_logs()
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
    inf_id = elt['inf']
    assert(isinstance(inf_id, str))
    if(len(inf_id)<5):
        logger.debug(f"data_quality;fresq;ill_formed_inf;{inf_id}")
    paysage_id_to_use = elt.get('paysage_id_to_use', '')
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
    #geolocalisations = []
    #if 'geolocalisations' in elt and isinstance(elt['geolocalisations'], list):
    #    geolocalisations = elt['geolocalisations']
    #for geolocalisation in geolocalisations:
    #    if isinstance(geolocalisation, dict) and geolocalisation.get('site_geolocalisation', {}).get('coordinates'):
    #        geoloc_s = geolocalisation['site_geolocalisation']['coordinates'].replace('"', '').split(',')
    #        if (len(geoloc_s) == 2) and ('[' in geoloc_s[0]) and (']' in geoloc_s[1]):
    #            longitude = float(geoloc_s[0].replace('[', ''))
    #            latitude = float(geoloc_s[1].replace(']', ''))
    #        else:
    #            logger.debug(f"data_quality;fresq;geoloc_ill_formed;{inf_id};{uai_fresq};{geoloc_s}")
    #monmaster
    #monmaster_infos = get_monmaster_elt(inf_id, uai_fresq) # todo ? use paysage ?
    #elt.update(monmaster_infos)
    
    #entity fishing
    #ef_infos = get_entityfishing()
    #elt.update(ef_infos)

    #sise
    list_code_sise_fresq = []
    raw_code_sise = []
    invalid_code_sise = []
    if elt.get('code_sise'):
        list_code_sise_fresq += get_clean_sise_code_as_list(elt['code_sise'])
        if isinstance(elt['code_sise'], str):
            raw_code_sise += [elt['code_sise']]
        elif isinstance(elt['code_sise'], list):
            raw_code_sise += elt['code_sise']
    elif 'formation_details' in elt:
        if isinstance(elt['formation_details'].get('parcours_diplomants_full'), list):
            for parcours in elt['formation_details'].get('parcours_diplomants_full'):
                if parcours.get('code_sise'):
                    if isinstance(parcours['code_sise'], str):
                        raw_code_sise += [parcours['code_sise']]
                    if isinstance(parcours['code_sise'], list):
                        raw_code_sise += parcours['code_sise']
                    list_code_sise_fresq += get_clean_sise_code_as_list(parcours['code_sise'])
    if list_code_sise_fresq:
        list_code_sise_fresq = list(set(list_code_sise_fresq))
        elt['code_sise_valid'] = list_code_sise_fresq
    if raw_code_sise:
        raw_code_sise = list(set(raw_code_sise))
        for raw_code in raw_code_sise:
            if raw_code not in list_code_sise_fresq:
                invalid_code_sise.append(raw_code)
    if invalid_code_sise:
        invalid_code_sise = list(set(invalid_code_sise))
        elt['code_sise_invalid'] = invalid_code_sise
        #logger.debug(f'code sise for {fresq_etab_id} : {list_code_sise_fresq}')
    #if len(list_code_sise_fresq) == 0:
        #logger.debug(f"data_quality;fresq;no_codeSISE;{inf_id};{paysage_id_to_use}")
    #sise_infos = {}
    #nb_has_sise_infos = 0
    #elt['has_sise_infos_years'] = []

    sise_infos = get_sise_elt(uais = elt['uais_etablissements'], inf = elt['inf'], annee = 'all')
    elt.update(sise_infos)

    num_rncps = []
    if isinstance(elt.get('num_rncp'), str) and 'RNCP' in elt['num_rncp']:
        num_rncps = [elt['num_rncp']]
    elif isinstance(elt.get('num_rncp'), list):
        num_rncps = elt['num_rncp']
    elif 'formation_details' in elt:
        if isinstance(elt['formation_details'].get('parcours_diplomants_full'), list):
            for parcours in elt['formation_details'].get('parcours_diplomants_full'):
                if parcours.get('num_rncp'):
                    num_rncps.append(str(parcours['num_rncp']))
    if len(num_rncps) == 0:
        assert(elt.get('num_rncp') is None)
        #logger.debug(f"data_quality;fresq;no_RNCP;{inf_id};{paysage_id_to_use}")

    # rncp
    rncp_infos = get_rncp_elt(num_rncps)
    elt.update(rncp_infos)

    # rome
    rome_infos = get_rome_elt(num_rncps)
    elt.update(rome_infos)
    
    return elt
