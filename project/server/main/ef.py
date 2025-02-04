import requests
import pandas as pd
import os
import json

from project.server.main.logger import get_logger
from project.server.main.utils import download_file, to_jsonl, get_filename
from project.server.main.utils_swift import upload_object, download_object
logger = get_logger(__name__)

def get_entityfishing():
    ans = {'has_entityfishing_infos': False, 'entityfishing_infos': {}}
    return ans
