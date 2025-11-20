import requests
import pandas as pd
import os
from project.server.main.logger import get_logger

logger = get_logger(__name__)

ODS_API_KEY = os.getenv('ODS_API_KEY')

def get_ods_data(key):
    logger.debug(f'getting ods data {key}')
    current_df = pd.read_csv(f'https://data.enseignementsup-recherche.gouv.fr/explore/dataset/{key}/download/?format=csv&apikey={ODS_API_KEY}', sep=';')
    return current_df

