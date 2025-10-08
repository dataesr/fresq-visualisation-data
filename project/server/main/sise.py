import pandas as pd
from project.server.main.logger import get_logger
logger = get_logger(__name__)

URL_SISE = 'https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-principaux-diplomes-et-formations-prepares-etablissements-publics/exports/csv?lang=en&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B'

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
        df_sise = pd.read_csv(URL_SISE, sep=';')
        logger.debug(f'reading {len(df_sise)} SISE data from ODS')
        df_sise.to_csv('sise_latest.csv.gz', index=False, sep=';')
    df_sise['DIPLOM'] = df_sise['DIPLOM'].apply(lambda x:str(x)).replace('.0', '')
    annees = df_sise['Année universitaire'].unique().tolist()
    annees.sort()
    years_in_sise = annees
    df_sise_dict = {}
    for a in annees:
        df_sise_dict[a] = df_sise[df_sise['Année universitaire']==a]
        logger.debug(f'for year {a}, nb SISE data = {len(df_sise_dict[a])}')
    return df_sise_dict, years_in_sise

def get_years_in_sise():
    global df_sise_dict, years_in_sise
    if df_sise_dict is None:
        df_sise_dict, years_in_sise = get_sise()
    return years_in_sise


#def get_sise_elt(uai_fresq, sise_fresq, annee, fresq_id):
def get_sise_elt(paysage_id_to_use, list_code_sise_fresq, annee, fresq_id):
    
    empty_ans = {'sise_matching': 'no_match', 'sise_infos': [],
            'has_sise_infos': False, 'code_sise_found': None, 'nb_code_sise_found': 0}
    
    if paysage_id_to_use is None:
        empty_ans['sise_matching'] = 'no_paysage_id'
        return empty_ans

    if list_code_sise_fresq is None or len(list_code_sise_fresq)==0:
        empty_ans['sise_matching'] = 'no_code_SISE'
        return empty_ans

    global df_sise_dict, years_in_sise
    if df_sise_dict is None:
        df_sise_dict, years_in_sise = get_sise()
    df_sise_annee = df_sise_dict[annee]

    method = 'code_sise_fresq'
    set_code_sise_fresq = set(list_code_sise_fresq)
    df_sise_filtered = df_sise_annee[df_sise_annee.DIPLOM.apply(lambda x: x in set_code_sise_fresq)]
    if len(df_sise_filtered) == 0:
        #logger.debug(f"data_quality;sise;codeSISE_absent_from_SISE_data;{fresq_id};{paysage_id_to_use};{'-'.join(list_code_sise_fresq)};{annee}")
        return empty_ans
        #method = 'libelle1_uai'
        #df_sise_filtered = df_sise_annee[df_sise_annee.index==mention_fresq]
        #df_test_code_sise = df_sise_filtered['DIPLOM'].value_counts()
        #if len(df_test_code_sise) != 1:
        #    df_sise_filtered = df_sise_annee[df_sise_annee['libelle_formation_1_2']==mention_fresq]
        #    method = 'libelle1_libelle2_uai'
        #    df_test_code_sise = df_sise_filtered['DIPLOM'].value_counts()
        #    if len(df_test_code_sise) != 1:
        #        return empty_ans

    columns_sise_map = {
            'Année universitaire': 'annee_universitaire',
            "Identifiant interne de l'établissement": "identifiant_interne_etablissement",
            "etablissement_compos_id_paysage": "etablissement_compos_id_paysage",
            "DIPLOM": "DIPLOM",
            "DEGETU": "DEGETU",
            "Degré d’études": "degre_etudes",
            "Diplôme": "diplome", 
            "Niveau dans le diplôme": "niveau_dans_le_diplome",
            'Libellé du diplôme ou de la formation 2': "libelle_formation_2",
            "implantation_code_commune": "implantation_code_commune",
            "Commune de l'unité d'inscription": "commune_unite_inscription",
            'Sélection disciplinaire': "selection_disciplinaire",
            'GD_DISCISCIPLINE': "grande_discipline_code",
            'Grande discipline': "grande_discipline",
            'DISCIPLINE': "discipline_code",
            'Discipline': "discipline",
            'SECT_DISCIPLINAIRE': "secteur_disciplinaire_code",
            'Secteur disciplinaire': "secteur_disciplinaire",
            "Nombre d'étudiants inscrits (inscriptions principales) hors doubles inscriptions CPGE": "nb_etudiants_inscrits_principales_hors_doubles_inscriptions_cpge",
            "Dont femmes": "dont_femmes",
            'Dont hommes': "dont_hommes",
            "Nombre d'étudiants inscrits (inscriptions principales) y compris doubles inscriptions CPGE": "nb_etudiants_inscrits_principales_y_compris_doubles_inscriptions_cpge",
            "Nombre total d'étudiants inscrits (inscriptions principales et secondes) hors double inscription CPGE": "nb_etudiants_inscrits_principales_secondes_hors_doubles_inscriptions_cpge",
            "Nombre total d'étudiants inscrits (inscriptions principales et secondes) y compris doubles inscriptions CPGE": "nb_etudiants_inscrits_principales_secondes_y_compris_doubles_inscriptions_cpge"
            }

    columns_sise = list(columns_sise_map.keys())

    #df_sise_final = df_sise_filtered[df_sise_filtered['Identifiant(s) UAI'] == uai_fresq][columns_sise]
    df_sise_final = df_sise_filtered[df_sise_filtered['etablissement_id_paysage_actuel'] == paysage_id_to_use][columns_sise]
    df_sise_final.columns = list(columns_sise_map.values())

    if len(df_sise_final) == 0:
        return empty_ans

    df_sise_final['DIPLOM'] = df_sise_final['DIPLOM'].apply(lambda x:str(x).replace('.0', ''))

    df_test_code_sise = df_sise_filtered['DIPLOM'].value_counts()
    assert(len(df_test_code_sise) <= len(list_code_sise_fresq))

    sise_discipline, sise_grande_discipline, sise_secteur_disciplinaire = [], [], []
    for k in ['discipline', 'grande_discipline', 'secteur_disciplinaire']:
        df_test = df_sise_final[k].value_counts()
        values = df_test.index.to_list()
        if k == 'discipline':
            sise_discipline = values
        if k == 'grande_discipline':
            sise_grande_discipline = values
        if k == 'secteur_disciplinaire':
            sise_secteur_disciplinaire = values
        if len(df_test) > 1:
            logger.debug(f"more than 1 {k}: {values} for {paysage_id_to_use};{'-'.join(list_code_sise_fresq)};{annee}")
            logger.debug(f"data_quality;sise;multiple_{k}_in_SISE;{fresq_id};{paysage_id_to_use};{'-'.join(list_code_sise_fresq)};{annee}")

    ans = df_sise_final.to_dict(orient='records')
    if ans:
        code_sise_found = df_test_code_sise.index.to_list()
        nb_code_sise_found = len(code_sise_found)
        return {f'sise_matching': method, f'sise_infos': ans, f'has_sise_infos': True,
                f'code_sise_found': code_sise_found, 'nb_code_sise_found': nb_code_sise_found,
                        'sise_discipline': sise_discipline, 'sise_grande_discipline': sise_grande_discipline,
                        'sise_secteur_disciplinaire': sise_secteur_disciplinaire
                       }
    return empty_ans


