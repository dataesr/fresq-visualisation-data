from project.server.main.logger import get_logger

logger = get_logger(__name__)

import os

client = None
from urllib import parse

from elasticsearch import Elasticsearch, helpers

ES_URL = os.getenv('ES_URL')
ES_LOGIN_FRESQ_BACK = os.getenv('ES_LOGIN_FRESQ_BACK')
ES_PASSWORD_FRESQ_BACK = os.getenv('ES_PASSWORD_FRESQ_BACK')

def get_es_host():
    es_url_without_http = ES_URL.replace('https://','').replace('http://','')
    es_host = f'https://{ES_LOGIN_FRESQ_BACK}:{parse.quote(ES_PASSWORD_FRESQ_BACK)}@{es_url_without_http}'
    return es_host

def get_client():
    global client
    if client is None:
        client = Elasticsearch(ES_URL, http_auth=(ES_LOGIN_FRESQ_BACK, ES_PASSWORD_FRESQ_BACK))
    return client

def get_filters() -> dict:
    return {
        'french_elision': {
            'type': 'elision',
            'articles_case': True,
            'articles': ['l', 'm', 't', 'qu', 'n', 's', 'j', 'd', 'c', 'jusqu', 'quoiqu', 'lorsqu', 'puisqu']
        },
        "french_stemmer": {
          "type": "stemmer",
          "language": "french"
        },
        "autocomplete_filter": {
          "type": "edge_ngram",
          "min_gram": 1,
          "max_gram": 10
        },
         "custom_grams": {
          "type": "edge_ngram",
          "min_gram": 6,
          "max_gram": 10,
          "preserve_original": True
        }
    }

def get_analyzers() -> dict:
    return {
        'light': {
            'tokenizer': 'icu_tokenizer',
            'filter': [
                'lowercase',
                'french_elision',
                'icu_folding'
            ]
        },
        'heavy': {
            'tokenizer': 'icu_tokenizer',
            'filter': [
                'lowercase',
                'french_elision',
                'icu_folding',
                'french_stemmer',
                'kstem',
                'custom_grams'
            ]
        },
        "autocomplete": {
          "type": "custom",
          "tokenizer": "icu_tokenizer",
          "filter": [
            "lowercase",
            'french_elision',
            'icu_folding',
            "autocomplete_filter"
          ]
        }
    }

def get_mappings_fresq():
    mappings = { 'properties': {} }
    mappings['properties']['has_sise_infos_years'] = {
                'type': 'text'
            }

    mappings['properties']['autocompleted'] = {
                #'type': 'search_as_you_type',
                #'analyzer': 'light'
                'type': 'text',
                'analyzer': 'autocomplete'
            }
    for f in ['mots_cles', 'specialites', 'domaines', 'domains',
              'domaine_rattachement_1_cti', 'domaine_rattachement_2_cti', 'domaine_rattachement_autre_cti',
              'specialite_sante', 'specialites_cti', 'nom_specialite_but',
              'intitule_officiel',
              'libelle_formation_2', 'mention_normalized',
              'nom_etablissement',
              'rncp_infos.type_emploi_accessibles',
              'rome_infos.level_1', 'rome_infos.level_2', 'rome_infos.level_3', 'rome_infos.label',
              'etablissements.libelle_etablissement', 'etablissements.nom_etablissement',
              'etablissements.denomination_etablissement', 'etablissements.nom_bce_etablissement', 'etablissements.paysage_name',
              'gd_disciscipline_lib',  'discipline_lib', 'sect_disciplinaire_lib', 'disciplines_selection',
              'formation_details.intitule_officiel',
              'formation_details.parcours_diplomants_full.intitule',
              'formation_details.etapes_details.informations_pedagogiques.mot_cle_libre',
              'formation_details.etapes_details.informations_pedagogiques.mot_cle_disciplinaire_details.nom',
              'formation_details.etapes_details.informations_pedagogiques.mot_cle_metier_details.nom',
              'formation_details.etapes_details.informations_pedagogiques.mot_cle_sectoriel_details.nom',
              'disciplinarySector', 'discipline_lib', 'disciplines_selection', 'engineeringSpecialties',
              'etapes.label', 'etapes.pedagogicalInfo.keywords', 'etapes.pedagogicalInfo.keywordsDisciplines',
              'etapes.pedagogicalInfo.keywordsSectors', 'etapes.pedagogicalInfo.keywordsJobs',
              'etablissements.name', 'etablissements.shortName', 'etablissements.sigle',
              'etablissements.paysageElt.name', 'etablissements.paysageEltToUse.name', 'etablissements.address.city',
              'locations.address.street', 'locations.address.city', 'locations.address.siteName', 'locations.name'
             ]:
        mappings['properties'][f] = {
                'type': 'text',
                'analyzer': 'heavy',
                'fields': {
                    'keyword': {
                        'type':  'keyword'
                    }
                }
            }
    for f in []:
        mappings['properties'][f] = {
                'type': 'text',
                'analyzer': 'light',
                'fields': {
                    'keyword': {
                        'type':  'keyword'
                    }
                }
            }
    for f in []:
        mappings['properties'][f] = {
                'type': 'text',
                'analyzer': 'light',
            }
    return mappings


def get_mappings_metiers():
    mappings = { 'properties': {} }

    mappings['properties']['autocompleted'] = {
                'type': 'text',
                'analyzer': 'autocomplete'
            }

    for f in ['label', 'level_1', 'level_2', 'metiers.label']:
        mappings['properties'][f] = {
                'type': 'text',
                'analyzer': 'heavy',
                'fields': {
                    'keyword': {
                        'type':  'keyword'
                    }
                }
            }
    return mappings


def get_mappings_mentions():
    mappings = { 'properties': {} }

    mappings['properties']['autocompleted'] = {
                'type': 'text',
                'analyzer': 'autocomplete'
            }

    for f in ['mots_cles', 'specialites', 'domaines',
              'domaine_rattachement_1_cti', 'domaine_rattachement_2_cti', 'domaine_rattachement_autre_cti',
              'intitule_officiel',
              'libelle_formation_2', 'mention_normalized',
              'formations.nom_etablissement',
              'rncp_infos.type_emploi_accessibles',
              'rome_infos.level_1', 'rome_infos.level_2', 'rome_infos.label',
              'sise_secteur_disciplinaire', 'sise_discipline', 'sise_grande_discipline',
          #    'entityfishing_infos.entities.domains',
          #    'monmaster_infos.listSpecialityCourse', 'monmaster_infos.keyWords',
          #   'monmaster_infos.courses.expectedSkills', 'monmaster_infos.courses.keyWords',
          #   'monmaster_infos.courses.criteres'
          ]:
        mappings['properties'][f] = {
                'type': 'text',
                'analyzer': 'heavy',
                'fields': {
                    'keyword': {
                        'type':  'keyword'
                    }
                }
            }
    return mappings

def get_mappings_etab():
    mappings = { 'properties': {} }

    mappings['properties']['autocompleted'] = {
                'type': 'text',
                'analyzer': 'autocomplete'
            }

    for f in ['denomination_etablissement', 'libelle_etablissement',
       'ville_etablissement', 'nom_etablissement_sort',
       'nom_commun_etablissement', 'nom_etablissement',
       'nom_bce_etablissement', 'nom_courant_etablissement',
      'sigle_etablissement',
       'libelle_avec_parent_etablissement']:
        mappings['properties'][f] = {
                'type': 'text',
                'analyzer': 'heavy',
                'fields': {
                    'keyword': {
                        'type':  'keyword'
                    }
                }
            }
    return mappings

def reset_index(index, mappings) -> None:
    es = get_client()
    delete_index(index)

    settings = {
        'analysis': {
            'filter': get_filters(),
            'analyzer': get_analyzers()
        },
        'max_ngram_diff': 10
    }

    response = es.indices.create(
        index=index,
        body={'settings': settings, 'mappings': mappings},
        ignore=400  # ignore 400 already exists code
    )
    if 'acknowledged' in response and response['acknowledged']:
        response = str(response['index'])
        logger.debug(f'Index mapping success for index: {response}')
    else:
        logger.debug(f'ERROR !')
        logger.debug(response)

def delete_index(index: str) -> None:
    logger.debug(f'Deleting {index}')
    es = get_client()
    response = es.indices.delete(index=index, ignore=[400, 404])
    logger.debug(response)

def refresh_index(index):
    logger.debug(f'Refreshing {index}')
    es = get_client()
    response = es.indices.refresh(index=index)
    logger.debug(response)

def update_alias(index_name, alias):
    es = get_client()
    next_index = index_name
    actions = []
    was = ''
    try:
        previous_index_list = list(es.indices.get_alias(index=alias).keys())
        previous_index = previous_index_list[0]
        actions.append({'remove': {'index': previous_index, 'alias': alias}})
        was = f'(was before {previous_index})'
    except:
        pass
    actions.append({'add': {'index': next_index, 'alias': alias}})
    es.indices.update_aliases(body={
        'actions': actions
    })
    logger.debug(f'set alias {alias} on index {index_name} {was}')

def update_all_aliases(suffix, alias_type):
    update_alias(f'fresq-mentions-{suffix}', f'fresq-mentions-{alias_type}')
    update_alias(f'fresq-etablissements-{suffix}', f'fresq-etablissements-{alias_type}')
    update_alias(f'fresq-metiers-{suffix}', f'fresq-metiers-{alias_type}')
    update_alias(f'fresq-{suffix}', f'fresq-{alias_type}')
