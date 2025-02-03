from jsonschema import exceptions, validate
import json
import pandas as pd
import os
import shutil
import re
import json
import string
import unicodedata
import requests

from tokenizers import normalizers
from tokenizers.normalizers import BertNormalizer, Sequence, Strip
from tokenizers import pre_tokenizers

from tokenizers import pre_tokenizers
from tokenizers.pre_tokenizers import Whitespace

from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger
logger = get_logger(__name__)

import datetime

def get_filename_from_cd(cd: str):
    """ Get filename from content-disposition """
    if not cd:
        return None
    fname = re.findall('filename=(.+)', cd)
    if len(fname) == 0:
        return None
    return fname[0]

def get_filename(url):
    with requests.get(url, stream=True, verify=False) as r:
        r.raise_for_status()
        try:
            local_filename = get_filename_from_cd(r.headers.get('content-disposition')).replace('"', '')
        except:
            local_filename = url.split('/')[-1]
    return local_filename

def download_file(url: str, upload_to_object_storage: bool = True, destination: str = None) -> str:
    start = datetime.datetime.now()
    with requests.get(url, stream=True, verify=False) as r:
        r.raise_for_status()
        try:
            local_filename = get_filename_from_cd(r.headers.get('content-disposition')).replace('"', '')
        except:
            local_filename = url.split('/')[-1]
        logger.debug(f'Start downloading {local_filename} at {start}')
        if destination:
            local_filename = destination
        with open(local_filename, 'wb') as f:
            shutil.copyfileobj(r.raw, f, length=16 * 1024 * 1024)
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'End download in {delta}')
    if upload_to_object_storage:
        upload_object(container='fresq', source=local_filename, target=local_filename)
    return local_filename

def get_today():
    return datetime.datetime.today().strftime('%Y%m%d')

def get_raw_data_filename(suffix):
    return f'fresq_raw_{suffix}.json.gz'

def get_transformed_data_filename(suffix):
    return f'fresq_transformed_{suffix}.jsonl'

def validate_json_schema(data: list, _schema: dict) -> bool:
    is_valid = True
    try:
        for datum in data:
            validate(instance=datum, schema=_schema)
    except exceptions.ValidationError as error:
        is_valid = False
        logger.error(error)
    return is_valid

def clean_json(elt):
    if isinstance(elt, dict):
        keys = list(elt.keys()).copy()
        for f in keys:
            if (not elt[f] == elt[f]) or (elt[f] is None):
                del elt[f]
            else:
                elt[f] = clean_json(elt[f])
    elif isinstance(elt, list):
        for ix, k in enumerate(elt):
            elt[ix] = clean_json(elt[ix])
    return elt

def clean_json_old(elt):
    keys = list(elt.keys()).copy()
    for f in keys:
        if isinstance(elt[f], dict):
            elt[f] = clean_json(elt[f])
        elif (not elt[f] == elt[f]) or (elt[f] is None):
            del elt[f]
        elif (isinstance(elt[f], str) and len(elt[f])==0):
            del elt[f]
        elif (isinstance(elt[f], list) and len(elt[f])==0):
            del elt[f]
    return elt

def to_jsonl(input_list, output_file, mode = 'a'):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            new = clean_json(entry)
            json.dump(new, outfile)
            outfile.write('\n')

def dedup_sort(x: list) -> list:
    y = list(set([e for e in x if e]))
    y.sort()
    return y


def remove_punction(s: str) -> str:
    for p in string.punctuation:
        s = s.replace(p, ' ').replace('  ', ' ')
    return s.strip()


def strip_accents(w: str) -> str:
    """Normalize accents and stuff in string."""
    return ''.join(
        c for c in unicodedata.normalize('NFD', w)
        if unicodedata.category(c) != 'Mn')


def delete_punct(w: str) -> str:
    """Delete all punctuation in a string."""
    return w.lower().translate(
        str.maketrans(string.punctuation, len(string.punctuation) * ' '))


normalizer = Sequence([BertNormalizer(clean_text=True,
        handle_chinese_chars=True,
        strip_accents=True,
        lowercase=True), Strip()])
pre_tokenizer = pre_tokenizers.Sequence([Whitespace()])


def normalize(x, remove_space = True, min_length = 0):
    if not isinstance(x, str):
        return ''
    normalized = normalizer.normalize_str(x)
    normalized = normalized.replace('\n', ' ')
    normalized = re.sub(' +', ' ', normalized)
    normalized = remove_punction(normalized)
    normalized = normalized.replace("’", "'")
    normalized = " ".join([e[0] for e in pre_tokenizer.pre_tokenize_str(normalized) if len(e[0]) > min_length])
    if remove_space:
        normalized = normalized.strip().replace(' ', '')
    return normalized
