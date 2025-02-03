from project.server.main.logger import get_logger
logger = get_logger(__name__)

import os

client = None
from elasticsearch import Elasticsearch, helpers

ES_URL = os.getenv('ES_URL')
ES_LOGIN_FRESQ_BACK = os.getenv('ES_LOGIN_FRESQ_BACK')
ES_PASSWORD_FRESQ_BACK = os.getenv('ES_PASSWORD_FRESQ_BACK')

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

def reset_index_fresq(index: str) -> None:
    es = get_client()
    delete_index(index)

    settings = {
        'analysis': {
            'filter': get_filters(),
            'analyzer': get_analyzers()
        },
        'max_ngram_diff': 10
    }
   
    mappings = { 'properties': {} }

    mappings['properties']['autocompleted'] = {
                #'type': 'search_as_you_type',
                #'analyzer': 'light'
                'type': 'text',
                'analyzer': 'autocomplete'
            }



    for f in ['mots_cles', 'specialites', 'domaines', 
              'domaine_rattachement_1_cti', 'domaine_rattachement_2_cti', 'domaine_rattachement_autre_cti',
              'specialite_sante', 'specialites_cti', 'nom_specialite_but',
              'intitule_officiel', 
              'libelle_formation_2', 'mention_normalized', 
              'nom_etablissement',
              'rncp_infos.type_emploi_accessibles',
              'rome_infos.level_1', 'rome_infos.level_2', 'rome_infos.level_3', 'rome_infos.label', 
              'sise_secteur_disciplinaire', 'sise_discipline', 'sise_grande_discipline',
              # 'entityfishing_infos.entities.domains',
              # 'monmaster_infos.listSpecialityCourse', 'monmaster_infos.keyWords',
             # 'monmaster_infos.courses.expectedSkills', 'monmaster_infos.courses.keyWords', 
             # 'monmaster_infos.courses.criteres'
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
