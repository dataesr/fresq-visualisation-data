import pandas as pd
from project.server.main.logger import get_logger
logger = get_logger(__name__)

URL_SISE = 'https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/fr-esr-principaux-diplomes-et-formations-prepares-etablissements-publics/exports/csv?lang=en&timezone=Europe%2FBerlin&use_labels=true&delimiter=%3B'

df_sise_dict, years_in_sise = None, []

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


def get_sise_elt(uai_fresq, sise_fresq, annee, fresq_id):
    
    empty_ans = {'sise_matching': 'no_match', 'sise_infos': [],
                         'has_sise_infos': False, 'code_sise_found': None}
    
    if sise_fresq is None:
        empty_ans['sise_matching'] = 'no_code_SISE'
        return empty_ans

    global df_sise_dict, years_in_sise
    if df_sise_dict is None:
        df_sise_dict, years_in_sise = get_sise()
    df_sise_annee = df_sise_dict[annee]

    method = 'code_sise_fresq'
    df_sise_filtered = df_sise_annee[df_sise_annee.DIPLOM==sise_fresq]
    if len(df_sise_filtered) == 0:
    #    logger.debug(f'code SISE {sise_fresq} absent from SISE data in {annee}')
    #    logger.debug(f"data_issue;codeSISE_absent_from_SISE_data;{fresq_id};{uai_fresq};{sise_fresq};{annee}")
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

    columns_sise = [
        'Année universitaire',
        "Identifiant interne de l'établissement", # Pour l’établissement
        "etablissement_compos_id_paysage", #Pour pouvoir gérer les EPE
        "DIPLOM",
        "DEGETU",
        "Degré d’études",
        "Diplôme",
        "Niveau dans le diplôme",
        'Libellé du diplôme ou de la formation 2',
        "implantation_code_commune",
        "Commune de l'unité d'inscription",
        'Sélection disciplinaire', 'GD_DISCISCIPLINE', 'Grande discipline',
       'DISCIPLINE', 'Discipline', 'SECT_DISCIPLINAIRE',
       'Secteur disciplinaire',
        "Nombre d'étudiants inscrits (inscriptions principales) hors doubles inscriptions CPGE",
        "Dont femmes", 'Dont hommes',
        "Nombre d'étudiants inscrits (inscriptions principales) y compris doubles inscriptions CPGE",
"Nombre total d'étudiants inscrits (inscriptions principales et secondes) hors double inscription CPGE",
"Nombre total d'étudiants inscrits (inscriptions principales et secondes) y compris doubles inscriptions CPGE",
    ]

    df_sise_final = df_sise_filtered[df_sise_filtered['Identifiant(s) UAI'] == uai_fresq][columns_sise]
    df_sise_final.columns = [
        'annee_universitaire',
        'identifiant_interne_etablissement',
        'etablissement_compos_id_paysage',
        'DIPLOM', 'DEGETU',
        'degre_etudes',
        'diplome', 'niveau_dans_le_diplome',
        'libelle_formation_2',
        'implantation_code_commune', 
        'commune_unite_inscription',
        'selection_disciplinaire', 'grande_discipline_code', 'grande_discipline',
       'discipline_code', 'discipline', 'secteur_disciplinaire_code',
       'secteur_disciplinaire',
        'nb_etudiants_inscrits_principales_hors_doubles_inscriptions_cpge', 'dont_femmes', 'dont_hommes',
        'nb_etudiants_inscrits_principales_y_compris_doubles_inscriptions_cpge',
        'nb_etudiants_inscrits_principales_secondes_hors_doubles_inscriptions_cpge',
        'nb_etudiants_inscrits_principales_secondes_y_compris_doubles_inscriptions_cpge'
    ]

    df_sise_final['DIPLOM'] = df_sise_final['DIPLOM'].apply(lambda x:str(x).replace('.0', ''))

    df_test_code_sise = df_sise_filtered['DIPLOM'].value_counts()
    assert(len(df_test_code_sise) <= 1)

    ans = df_sise_final.to_dict(orient='records')
    if ans:
        code_sise_found = df_test_code_sise.index[0]
        sise_discipline = df_sise_final.head(1).discipline.values[0]
        sise_grande_discipline = df_sise_final.head(1).grande_discipline.values[0]
        sise_secteur_disciplinaire = df_sise_final.head(1).secteur_disciplinaire.values[0]
        return {f'sise_matching': method, f'sise_infos': ans, f'has_sise_infos': True,
                f'code_sise_found': code_sise_found,
                        'sise_discipline': sise_discipline, 'sise_grande_discipline': sise_grande_discipline,
                        'sise_secteur_disciplinaire': sise_secteur_disciplinaire
                       }
    return empty_ans


