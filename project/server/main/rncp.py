from project.server.main.logger import get_logger
logger = get_logger(__name__)

def get_rncp():
    return {}

def get_rncp_elt(num_rncp):
    df_tmp = df_rncp[df_rncp.index==num_rncp]#['type_emploi_accessibles']
    ans = {'has_rncp_infos': False, 'rncp_infos': {}}
    rncp_infos = {}
    if len(df_tmp)>0:
        elt = df_tmp.to_dict(orient='records')[0]
        for f in ['type_emploi_accessibles']:
            rncp_infos[f] = elt[f]
        return {'has_rncp_infos': True, 'rncp_infos': rncp_infos}
    return ans

def get_rome_elt(num_rncp):
    ans = {'has_rome_infos': False, 'rome_infos': {}}
    if num_rncp in rncp2rome:
        return {'has_rome_infos': True, 'rome_infos': rncp2rome[num_rncp]}
    return ans

def parse_fiche_rncp(e):
    elt = {}
    elt['label'] = get_value(e, 'INTITULE')
    for f in ['ID_FICHE', 'NUMERO_FICHE', 'ANCIENNE_CERTIFICATION', 'ETAT_FICHE', 
              'ACTIVITES_VISEES', 'REGLEMENTATIONS_ACTIVITES',
              'CAPACITES_ATTESTEES', 'SECTEURS_ACTIVITE', 'TYPE_EMPLOI_ACCESSIBLES', 'OBJECTIFS_CONTEXTE',
              'ACTIF', 'DATE_DE_PUBLICATION']:
        elt[f.lower()] = get_value(e, f)
    certificateurs = []
    if e.find('CERTIFICATEURS'):
        certificateurs_xml = e.find('CERTIFICATEURS').find_all('CERTIFICATEUR')
        for c in certificateurs_xml:
            certif = {}
            siret = get_value(c, 'SIRET_CERTIFICATEUR')
            label = get_value(c, 'NOM_CERTIFICATEUR')
            certif['siret'] = siret
            certif['label'] = label
            certificateurs.append(certif)
    elt['certificateurs'] = certificateurs
    elt['nb_certificateurs'] = len(certificateurs)

    partenaires = []
    if e.find('PARTENAIRES'):
        partenaires_xml = e.find('PARTENAIRES').find_all('PARTENAIRE')
        for p in partenaires_xml:
            part = {}
            siret = get_value(p, 'SIRET_PARTENAIRE')
            label = get_value(p, 'NOM_PARTENAIRE')
            habilitation = get_value(p, 'HABILITATION_PARTENAIRE')
            status_habilitation = get_value(p, 'ETAT_PARTENAIRE')
            start_habilitation = get_value(p, 'DATE_ACTIF')
            updated_habilitation = get_value(p, 'DATE_DERNIERE_MODIFICATION_ETAT')
            part['siret'] = siret
            part['label'] = label
            part['habilitation'] = {'label': habilitation, 
                                    'status': status_habilitation, 
                                    'start_date': start_habilitation,
                                     'last_status_update': updated_habilitation}
            partenaires.append(part)
        #elt['partenaires'] = partenaires
    elt['nb_partenaires'] = len(partenaires)
    
    elt['activites_visees'] = get_value(e, '')
    
    romes = []
    if e.find('CODES_ROME'):
        codes_rome = e.find('CODES_ROME').find_all('ROME')
        for c in codes_rome:
            code = get_value(c, 'CODE')
            label = get_value(c, 'LIBELLE')
            rome = {'code': code, 'label': label}
            romes.append(rome)
    elt['codes_rome'] = romes
    
    competences = []
    if e.find('BLOCS_COMPETENCES'):
        bloc_comp = e.find('BLOCS_COMPETENCES').find_all('BLOC_COMPETENCES')
        for b in bloc_comp:
            competence = {}
            for f in ['CODE', 'LISTE_COMPETENCES', 'MODALITES_EVALUATION']:
                competence[f.lower()] = get_value(b, f)
            competence['label'] = get_value(b, 'LIBELLE')
            competences.append(competence)
    elt['competences'] = competences
    
    return elt

def get_value(x, l):
    n = x.find(l)
    if n:
        return n.get_text()
    return None

def get_rncp2rome():
    return {}
