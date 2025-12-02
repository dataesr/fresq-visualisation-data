import json
import math
import os
import requests
from retry import retry
import pandas as pd
import datetime
import time

from project.server.main.utils import get_today, save_logs
from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object, download_object

logger = get_logger(__name__)

def clean_etapes(data):
    ans = []
    for d in data:
        if 'formation_details' not in d:
            ans.append(d)
            continue
        if 'etapes_details' not in d['formation_details']:
            ans.append(d)
            continue
        for ix_etape, e in enumerate(d['formation_details']['etapes_details']):
            if 'references' in e:
                references = e['references']
                new_info = transform_references(references)
                for f in new_info:
                    existing_info = e.get(f, {})
                    logger.debug(f)
                    logger.debug(existing_info)
                    logger.debug('---')
                    logger.debug(new_info[f])
                    if existing_info:
                        existing_info.update(new_info[f])
                    elif new_info[f]:
                        existing_info = new_info[f]
                    e[f] = existing_info
                del e['references']
        ans.append(d)
    return ans

def get_list_data(my_dict, my_key):
    ans = []
    for f in my_dict:
        if my_key in f:
            if 'data' in my_dict[f]:
                new_elt = {'raw_key_from_fresq': f }
                for k in my_dict[f]['data']:
                    #if k[-3:] != '_id' and k[-4:] != '_ids':
                    new_elt[k] = my_dict[f]['data'][k]
                ans.append(new_elt)
    if ans:
        return ans
    return {}


def transform_references(references):
    ans = {}
    for f in ['uai_iut', 'uai_parent']:
        if f in references:
            ans[f'{f}_details'] = references[f]['data']
    for f in ['sites', 'modalite_enseignement']:
        current_details = get_list_data(references, f)
        if current_details:
            ans[f'{f}_details'] = current_details
    ans_ip = {}
    for f in ['mot_cle_sectoriel', 'mot_cle_disciplinaire', 'mot_cle_metier']:
        current_details = get_list_data(references, f)
        if current_details:
            ans_ip[f'{f}_details'] = current_details
    ans.update({'informations_pedagogiques': ans_ip})
    ans_recrut = {}
    for f in ['diplome_conseille', 'critere_examen_candidature']:
        current_details = get_list_data(references, f)
        if current_details:
            ans_recrut[f'{f}_details'] = current_details
    ans.update({'modalite_recrutement': ans_recrut})
    return ans
