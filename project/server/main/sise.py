import pandas as pd
from project.server.main.ods import get_ods_data
from project.server.main.logger import get_logger
logger = get_logger(__name__)

#URL_SISE = 'https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-principaux-diplomes-et-formations-prepares-etablissements-publics/exports/csv?lang=en&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B'

df_sise_dict, years_in_sise = None, []

def get_clean_sise_code_as_list(x):
    ans = []
    if isinstance(x, list):
        for c in x:
            ans += get_clean_sise_code_as_list(c)
    else:
        y = str(x).strip()
        if len(y) != 7:
            logger.debug(f'UNEXPECTED SISE: ;{x};')
        for k in y.replace(',', ' ').replace(';', ' ').split(' '):
            if len(k.strip()) == 7:
                ans.append(k)
    return ans

def get_sise():
    logger.debug('>>>>> get SISE >>>>>')
    try:
        df_sise = pd.read_csv('sise_latest.csv.gz', sep=';')
        logger.debug(f'reading {len(df_sise)} SISE data from local file')
    except:
        df_sise = get_ods_data('fr-esr-principaux-diplomes-et-formations-prepares-fresq')
        #df_sise = pd.read_csv(URL_SISE, sep=';')
        logger.debug(f'reading {len(df_sise)} SISE data from ODS')
        df_sise.to_csv('sise_latest.csv.gz', index=False, sep=';')
    annees = df_sise['annee_universitaire'].unique().tolist()
    annees.sort()
    years_in_sise = annees
    df_sise_dict = {}
    df_sise_dict['all'] = df_sise
    for a in annees:
        df_sise_dict[a] = df_sise[df_sise['annee_universitaire']==a]
    return df_sise_dict, years_in_sise

def get_years_in_sise():
    global df_sise_dict, years_in_sise
    if df_sise_dict is None:
        df_sise_dict, years_in_sise = get_sise()
    return years_in_sise

#def get_sise_elt(uai_fresq, sise_fresq, annee, fresq_id):
def get_sise_elt(uais, inf, annee):
    
    empty_ans = {'avec_sise_infos': False}
    
    if (uais is None) or len(uais)==0:
        empty_ans['sise_matching'] = 'no_uai'
        return empty_ans

    global df_sise_dict, years_in_sise
    if df_sise_dict is None:
        df_sise_dict, years_in_sise = get_sise()

    df_sise_annee = df_sise_dict[annee]
    
    filter_uai = df_sise_annee.uai_fresq.fillna('').str.split('/').apply(lambda x: any(u in uais for u in x))
    filter_inf = (df_sise_annee.inf.fillna('').str.split('/').apply(lambda x: inf in x))
    df_sise_filtered = df_sise_annee[filter_uai & filter_inf]
    if len(df_sise_filtered) == 0:
        return empty_ans

    df_sise_final = df_sise_filtered

    if len(df_sise_final) == 0:
        return empty_ans
    ans = {'avec_sise_infos': True}
    for k in ['gd_disciscipline_lib',  'discipline_lib', 'sect_disciplinaire_lib', 'disciplines_selection']:
        df_test = df_sise_final[k].value_counts()
        values = df_test.index.to_list()
        ans[k] = values
    return ans



