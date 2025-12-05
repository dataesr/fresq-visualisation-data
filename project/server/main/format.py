import json
import os
import re
from typing import Any, Dict, List, Literal, Optional, Set, Tuple, TypedDict, Union

from project.server.main.logger import get_logger
from project.server.main.transform import get_transformed_data
from project.server.main.utils import get_formatted_data_filename, save_logs, to_jsonl
from project.server.main.utils_swift import download_object, upload_object

logger = get_logger(__name__)

fresq_enriched = None

LocationType = Literal['etablissement', 'site']


class GeoPoint(TypedDict, total=False):
    type: str  # 'Point'
    coordinates: List[float]  # [longitude, latitude]


class Address(TypedDict, total=False):
    street: Optional[str]
    streetLine2: Optional[str]
    postalCode: Optional[str]
    city: Optional[str]
    siteName: Optional[str]


class Location(TypedDict, total=False):
    id: str
    uai: Optional[str]
    name: str
    address: Optional[Address]
    geo: Optional[GeoPoint]
    types: List[LocationType]


class TeachingModality(TypedDict, total=False):
    code: str
    label: str


class PaysageInfo(TypedDict, total=False):
    id: str
    name: str
    geoloc: Optional[str]
    uaiToPaysageMethod: Optional[str]


class Etablissement(TypedDict, total=False):
    uai: str
    name: str
    shortName: Optional[str]
    sigle: Optional[str]
    siret: Optional[str]
    nature: Optional[str]
    sector: str
    status: Optional[str]
    juridicalCategory: Optional[str]
    types: Optional[List[str]]
    groups: Optional[List[str]]
    supervisoryMinistries: Optional[List[str]]
    level: Optional[str]
    address: Optional[Address]
    geo: Optional[GeoPoint]
    phone: Optional[str]
    typeDelivrance: str
    coaccredited: Optional[List[Dict[str, str]]]
    academy: str
    region: str
    wave: Optional[str]
    locationIds: Optional[List[str]]
    paysageElt: Optional[PaysageInfo]
    paysageEltToUse: Optional[PaysageInfo]


class PedagogicalInfo(TypedDict, total=False):
    keywords: Optional[List[str]]
    keywordsDisciplines: Optional[List[str]]
    keywordsJobs: Optional[List[str]]
    keywordsSectors: Optional[List[str]]
    languages: Optional[List[str]]
    teachingLanguages: Optional[List[str]]
    formationLink: Optional[str]
    pedagogicalEmail: Optional[str]
    administrativeEmail: Optional[str]


class RecruitmentInfo(TypedDict, total=False):
    expectations: Optional[List[str]]
    recommendedDiplomas: Optional[List[str]]
    examCriteria: Optional[List[str]]
    selectionMethods: Optional[List[str]]


class Etape(TypedDict, total=False):
    infe: str
    label: str
    level: Optional[str]
    openingYear: Optional[int]
    isDiplomante: bool
    isOpen: bool
    siteIds: List[str]
    teachingModalities: List[TeachingModality]
    pedagogicalInfo: Optional[PedagogicalInfo]
    recruitmentInfo: Optional[RecruitmentInfo]
    capacity: Optional[int]


class Parcours(TypedDict, total=False):
    infp: str
    label: str
    sigle: Optional[str]
    rncp: Optional[str]
    codeSise: Optional[Union[str, int]]
    openingYear: Optional[int]
    isDiplomante: bool
    isOpen: bool
    cursus: Optional[List[List[str]]]


class Accreditation(TypedDict, total=False):
    startDate: Optional[str]
    endDate: Optional[str]
    endYears: Optional[List[str]]
    gradeEndDate: Optional[str]
    visaEndDate: Optional[str]


class Diploma(TypedDict, total=False):
    code: str
    type: str
    category: str
    order: Optional[int]


class RNCPInfo(TypedDict, total=False):
    rncp: str
    typeEmploiAccessibles: Optional[str]


class ROMEInfo(TypedDict, total=False):
    codeRome: str
    idLevel1: str
    level1: str
    idLevel2: str
    level2: str
    level3: str
    label: str
    ogr: str
    rncp: Optional[str]


class FormationFormatted(TypedDict, total=False):
    inf: str
    label: str
    mentionNormalized: str
    mentionId: str
    cycle: str
    diploma: Diploma
    accreditation: Accreditation
    domains: Optional[List[str]]
    codeSise: Optional[Union[str, List[str]]]
    codeSiseValid: Optional[List[str]]
    codeSiseInvalid: Optional[List[str]]
    rncp: Optional[str]
    qualificationLevel: Optional[str]
    teachingModalities: Optional[List[str]]
    healthCycle: Optional[str]
    healthSpecialty: Optional[str]
    engineeringSpecialties: Optional[List[str]]
    butType: Optional[str]
    butSpecialtySigle: Optional[str]
    disciplinarySector: Optional[str]
    keywords: Optional[List[str]]
    etablissements: List[Etablissement]
    parcours: List[Parcours]
    etapes: List[Etape]
    locations: List[Location]
    collectionId: Optional[str]
    recordId: Optional[str]
    bucketId: Optional[str]
    sourceId: Optional[str]
    # Enrichment fields (renamed, not enriched)
    rncpInfos: Optional[List[RNCPInfo]]
    hasRncpInfos: bool
    romeInfos: Optional[List[ROMEInfo]]
    hasRomeInfos: bool
    siseInfos: Optional[Dict[str, Any]]
    hasSiseInfos: bool


class LocationCollector:
    """Collects and deduplicates locations with type merging."""

    def __init__(self):
        self.locations: Dict[str, Location] = {}

    def _generate_id(self, id: Optional[str] = None,
                     coords: Optional[Tuple[float, float]] = None,
                     name: Optional[str] = None) -> str:
        """Generate a unique ID for a location."""
        if id:
            return id
        if coords and len(coords) == 2:
            return f"{coords[0]:.6f},{coords[1]:.6f}"
        return name or 'unknown'

    def _parse_coordinates(self, coords: Optional[Union[str, List[float]]]) -> Optional[Tuple[float, float]]:
        """Parse coordinates from various formats."""
        if not coords:
            return None

        if isinstance(coords, (list, tuple)) and len(coords) == 2:
            try:
                return (float(coords[0]), float(coords[1]))
            except (ValueError, TypeError):
                return None

        if isinstance(coords, str):
            try:
                parsed = json.loads(coords)
                if isinstance(parsed, list) and len(parsed) == 2:
                    return (float(parsed[0]), float(parsed[1]))
            except (json.JSONDecodeError, ValueError, TypeError):
                pass

            # Try regex match
            match = re.match(r'\[?\s*(-?\d+\.?\d*)\s*,\s*(-?\d+\.?\d*)\s*\]?', coords)
            if match:
                return (float(match.group(1)), float(match.group(2)))

        return None

    def _parse_paysage_geoloc(self, geoloc: Optional[str]) -> Optional[Tuple[float, float]]:
        """Parse paysage geoloc format: 'name###lat###lon'."""
        if not geoloc or not isinstance(geoloc, str):
            return None
        parts = geoloc.split('###')
        if len(parts) >= 3:
            try:
                lat = float(parts[1])
                lon = float(parts[2])
                return (lon, lat)  # Return as (lon, lat) for GeoJSON
            except (ValueError, IndexError):
                pass
        return None

    def add(self, location_type: LocationType,
            id: Optional[str] = None,
            name: str = "",
            address: Optional[Address] = None,
            geo_coords: Optional[Union[str, List[float], Tuple[float, float]]] = None) -> str:
        """Add a location and return its ID."""
        coords = None
        if isinstance(geo_coords, tuple) and len(geo_coords) == 2:
            coords = geo_coords
        else:
            coords = self._parse_coordinates(geo_coords)

        location_id = self._generate_id(id, coords, name)

        existing = self.locations.get(location_id)
        if existing:
            # Merge types
            existing_types = existing.get('types', [])
            if location_type not in existing_types:
                existing_types.append(location_type)
                existing['types'] = existing_types
            # Merge address if more complete
            existing_address = existing.get('address')
            if address and (not existing_address or not existing_address.get('street')):
                merged_address: Address = {}
                if existing_address:
                    merged_address.update(existing_address)
                merged_address.update(address)
                existing['address'] = merged_address
            return location_id

        location: Location = {
            'id': location_id,
            'name': name,
            'types': [location_type]
        }

        if address:
            location['address'] = address
        if coords and len(coords) == 2:
            location['geo'] = {'type': 'Point', 'coordinates': list(coords)}

        self.locations[location_id] = location
        return location_id

    def add_from_site_details(self, site: Dict[str, Any]) -> str:
        """Add location from sites_details (output of clean_etapes)."""
        addr = site.get('adresse', {})

        address: Address = {}
        if addr.get('ligne1'):
            address['street'] = addr.get('ligne1')
        if addr.get('ligne2'):
            address['streetLine2'] = addr.get('ligne2')
        if addr.get('code_postal'):
            address['postalCode'] = addr.get('code_postal')
        if addr.get('ville'):
            address['city'] = addr.get('ville')
        if site.get('nom'):
            address['siteName'] = site.get('nom') or site.get('nom_site')

        geo_coords = None
        if addr.get('geolocalisation') and addr['geolocalisation'].get('coordinates'):
            geo_coords = addr['geolocalisation']['coordinates']

        return self.add(
            'site',
            id=site.get('uai') or addr.get('uai'),
            name=addr.get('nom_site') or addr.get('nom') or addr.get('nom_fresq') or addr.get('ville') or 'Site',
            address=address if address else None,
            geo_coords=geo_coords
        )

    def add_from_paysage(self, paysage_elt: Dict[str, Any]) -> str:
        """Add location from paysage element using its geoloc."""
        geo_coords = self._parse_paysage_geoloc(paysage_elt.get('geoloc'))

        return self.add(
            'etablissement',
            id=paysage_elt.get('id'),
            name=paysage_elt.get('name', ''),
            geo_coords=geo_coords
        )

    def get_all(self) -> List[Location]:
        """Get all collected locations."""
        return list(self.locations.values())

    def get_ids(self) -> List[str]:
        """Get all location IDs."""
        return list(self.locations.keys())


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def extract_teaching_modalities(etape: Dict[str, Any]) -> List[TeachingModality]:
    """Extract teaching modalities from etape (after clean_etapes)."""
    modalities: List[TeachingModality] = []
    seen: Set[str] = set()

    modal_details = etape.get('modalite_enseignement_details', [])
    if isinstance(modal_details, list):
        for modal in modal_details:
            code = modal.get('code')
            if code and code not in seen:
                seen.add(code)
                modalities.append({
                    'code': code,
                    'label': modal.get('libelle', code)
                })

    return modalities


def extract_mot_cle(etape: Dict[str, Any], keyword_type) -> List[str]:
    """Extract disciplines from etape (after clean_etapes)."""
    disciplines: List[str] = []

    ped_info = etape.get('informations_pedagogiques', {})
    disc_details = ped_info.get(keyword_type, [])
    if isinstance(disc_details, list):
        for disc in disc_details:
            name = disc.get('nom', '')
            name = re.sub(r'^-+\s*|\s*-+$', '', name).strip()
            if name and name not in disciplines:
                disciplines.append(name)

    return disciplines

def extract_recommended_diplomas(etape: Dict[str, Any]) -> List[str]:
    """Extract recommended diplomas from etape (after clean_etapes)."""
    diplomas: List[str] = []

    # After clean_etapes, diplomas are in modalite_recrutement.diplome_conseille_details
    rec_info = etape.get('modalite_recrutement', {})
    dipl_details = rec_info.get('diplome_conseille_details', [])
    if isinstance(dipl_details, list):
        for dipl in dipl_details:
            intitule = dipl.get('intitule')
            if intitule and intitule not in diplomas:
                diplomas.append(intitule)

    return diplomas


def extract_selection_methods(etape: Dict[str, Any]) -> List[str]:
    """Extract selection methods from etape (after clean_etapes)."""
    methods: List[str] = []

    # After clean_etapes, methods are in modalite_recrutement.critere_examen_candidature_details
    rec_info = etape.get('modalite_recrutement', {})
    crit_details = rec_info.get('critere_examen_candidature_details', [])
    if isinstance(crit_details, list):
        for crit in crit_details:
            libelle = crit.get('libelle')
            if libelle and libelle not in methods:
                methods.append(libelle)

    return methods


# ============================================================================
# FIELD RENAMING FUNCTIONS
# ============================================================================

def rename_paysage_elt(paysage_elt: Optional[Dict[str, Any]]) -> Optional[PaysageInfo]:
    """Rename paysage element fields to camelCase."""
    if not paysage_elt:
        return None

    result: PaysageInfo = {
        'id': paysage_elt.get('id', ''),
        'name': paysage_elt.get('name', '')
    }
    if paysage_elt.get('geoloc'):
        result['geoloc'] = paysage_elt['geoloc']
    if paysage_elt.get('uai_to_paysage_method'):
        result['uaiToPaysageMethod'] = paysage_elt['uai_to_paysage_method']

    return result


def are_paysage_elts_same(elt1: Optional[Dict[str, Any]], elt2: Optional[Dict[str, Any]]) -> bool:
    """Check if two paysage elements are the same (by id and geoloc)."""
    if not elt1 and not elt2:
        return True
    if not elt1 or not elt2:
        return False
    return elt1.get('id') == elt2.get('id') and elt1.get('geoloc') == elt2.get('geoloc')


def rename_rncp_infos(rncp_infos: Optional[List[Dict[str, Any]]]) -> Optional[List[RNCPInfo]]:
    """Rename RNCP info fields to camelCase."""
    if not rncp_infos:
        return None

    result: List[RNCPInfo] = []
    for info in rncp_infos:
        renamed: RNCPInfo = {
            'rncp': info.get('rncp', '')
        }
        if info.get('type_emploi_accessibles'):
            renamed['typeEmploiAccessibles'] = info['type_emploi_accessibles']
        result.append(renamed)

    return result


def rename_rome_infos(rome_infos: Optional[List[Dict[str, Any]]]) -> Optional[List[ROMEInfo]]:
    """Rename ROME info fields to camelCase."""
    if not rome_infos:
        return None

    result: List[ROMEInfo] = []
    for info in rome_infos:
        renamed: ROMEInfo = {
            'codeRome': info.get('code_rome', ''),
            'idLevel1': info.get('id_level_1', ''),
            'level1': info.get('level_1', ''),
            'idLevel2': info.get('id_level_2', ''),
            'level2': info.get('level_2', ''),
            'level3': info.get('level_3', ''),
            'label': info.get('label', ''),
            'ogr': info.get('ogr', '')
        }
        if info.get('rncp'):
            renamed['rncp'] = info['rncp']
        result.append(renamed)

    return result


# ============================================================================
# TRANSFORMATION FUNCTIONS
# ============================================================================

def transform_etape(raw: Dict[str, Any], location_collector: LocationCollector) -> Etape:
    """Transform a raw etape into the target format with location references.

    Uses sites_details from clean_etapes output.
    """
    site_ids: List[str] = []

    # Collect site locations from sites_details for BUT
    sites_details = raw.get('sites_details', [])
    if isinstance(sites_details, list):
        for site in sites_details:
            site_id = location_collector.add_from_site_details(site)
            if site_id not in site_ids:
                site_ids.append(site_id)

    # Collect site locations from sites_details for MASTERS
    sites = raw.get('sites', [])
    if isinstance(sites, list):
        for site in sites:
            site_id = location_collector.add_from_site_details(site)
            if site_id not in site_ids:
                site_ids.append(site_id)



    keywords_disciplines = extract_mot_cle(raw, 'mot_cle_disciplinaire_details')
    keywords_metiers = extract_mot_cle(raw, 'mot_cle_metier_details')
    keywords_secteurs = extract_mot_cle(raw, 'mot_cle_sectoriel_details')
    recommended_diplomas = extract_recommended_diplomas(raw)
    selection_methods = extract_selection_methods(raw)

    etape: Etape = {
        'infe': raw.get('inf_e', ''),
        'label': raw.get('intitule', ''),
        'level': raw.get('niveau'),
        'openingYear': raw.get('annee_ouverture'),
        'isDiplomante': raw.get('ind_etape_diplomante', False) or False,
        'isOpen': raw.get('ind_formation_ouverte', False) or False,
        'siteIds': site_ids,
        'teachingModalities': extract_teaching_modalities(raw)
    }

    # Pedagogical info
    ped_info = raw.get('informations_pedagogiques', {})
    if ped_info:
        pedagogical_info: PedagogicalInfo = {}
        if ped_info.get('mot_cle_libre'):
            pedagogical_info['keywords'] = ped_info['mot_cle_libre']
        if keywords_disciplines:
            pedagogical_info['keywordsDisciplines'] = keywords_disciplines
        if keywords_metiers:
            pedagogical_info['keywordsJobs'] = keywords_metiers
        if keywords_secteurs:
            pedagogical_info['keywordsSectors'] = keywords_secteurs
        if ped_info.get('langues_vivantes'):
            pedagogical_info['languages'] = ped_info['langues_vivantes']
        if ped_info.get('langues_enseignement'):
            pedagogical_info['teachingLanguages'] = ped_info['langues_enseignement']
        if ped_info.get('lien_fiche_formation'):
            pedagogical_info['formationLink'] = ped_info['lien_fiche_formation']
        if ped_info.get('email_contact_pedagogique'):
            pedagogical_info['pedagogicalEmail'] = ped_info['email_contact_pedagogique']
        if ped_info.get('email_contact_administratif'):
            pedagogical_info['administrativeEmail'] = ped_info['email_contact_administratif']
        if pedagogical_info:
            etape['pedagogicalInfo'] = pedagogical_info

    # Recruitment info
    rec_info = raw.get('modalite_recrutement', {})
    if rec_info or recommended_diplomas or selection_methods:
        recruitment_info: RecruitmentInfo = {}
        if rec_info.get('attendus'):
            recruitment_info['expectations'] = rec_info['attendus']
        if recommended_diplomas:
            recruitment_info['recommendedDiplomas'] = recommended_diplomas
        if rec_info.get('criteres_generaux_examen'):
            recruitment_info['examCriteria'] = rec_info['criteres_generaux_examen']
        if selection_methods:
            recruitment_info['selectionMethods'] = selection_methods
        if recruitment_info:
            etape['recruitmentInfo'] = recruitment_info

    # Capacity
    capacite_accueil = raw.get('capacite_accueil', {})
    if capacite_accueil and 'cal' in capacite_accueil:
        cal = capacite_accueil['cal']
        if isinstance(cal, (int, float)) and not isinstance(cal, bool):
            etape['capacity'] = int(cal)
        else:
            etape['capacity'] = None

    return etape


def transform_parcours(raw: Dict[str, Any]) -> Parcours:
    """Transform a raw parcours into the target format."""
    parcours: Parcours = {
        'infp': raw.get('inf_p', ''),
        'label': raw.get('intitule', ''),
        'sigle': raw.get('sigle'),
        'rncp': raw.get('num_rncp'),
        'codeSise': raw.get('code_sise'),
        'openingYear': raw.get('annee_ouverture'),
        'isDiplomante': True,
        'isOpen': raw.get('ind_formation_ouverte', False) or False
    }

    raw_cursus = raw.get('cursus', [[]])
    etape_diplomante = raw.get('etape_diplomante')
    if raw_cursus and etape_diplomante:
        cursus_with_etape = []
        for c in raw_cursus:
            if isinstance(c, list):
                cursus_with_etape.append(c + [etape_diplomante])
            else:
                cursus_with_etape.append([c, etape_diplomante])
        parcours['cursus'] = cursus_with_etape
    else:
        parcours['cursus'] = raw_cursus

    return parcours


def build_etablissement(data: Dict[str, Any], location_collector: LocationCollector) -> Etablissement:
    """Build an etablissement from raw data"""


    etab: Etablissement = {
        'uai': data.get('uai_etablissement', ''),
        'name': data.get('nom_etablissement') or data.get('nom_commun_etablissement') or '',
        'shortName': data.get('libelle_etablissement'),
        'sigle': data.get('sigle_etablissement'),
        'siret': data.get('siret_etablissement'),
        'nature': data.get('nature_etablissement'),
        'sector': data.get('secteur', ''),
        'status': data.get('etat_etablissement'),
        'juridicalCategory': data.get('categorie_judiciaire_etablissement'),
        'types': data.get('types_etablissement'),
        'groups': data.get('groupes_etablissement'),
        'supervisoryMinistries': data.get('ministeres_tutelle'),
        'level': data.get('niveau_etablissement'),
        'wave': data.get('vague'),
        'address': {
            'postalCode': data.get('code_postal_etablissement'),
            'city': data.get('ville_etablissement')
        },
        'phone': None,
        'typeDelivrance': data.get('type_delivrance', ''),
        'academy': data.get('academie', ''),
        'region': data.get('region_academique', ''),
    }

    paysage_elt = data.get('paysage_elt')
    paysage_elt_to_use = data.get('paysage_elt_to_use')

    location_ids = []
    # Always include paysageElt if it exists
    if paysage_elt:
        etab['paysageElt'] = rename_paysage_elt(paysage_elt)
        location_id = location_collector.add_from_paysage(paysage_elt)
        location_ids.append(location_id)

    # Only include paysageEltToUse if it's different from paysageElt
    if paysage_elt_to_use and not are_paysage_elts_same(paysage_elt, paysage_elt_to_use):
        etab['paysageEltToUse'] = rename_paysage_elt(paysage_elt_to_use)
        location_id_to_use = location_collector.add_from_paysage(paysage_elt_to_use)
        location_ids.append(location_id_to_use)

    etab['locationIds'] = location_ids

    return etab


def format_record(data: Dict[str, Any]) -> FormationFormatted:
    """Format a single FRESQ record with renamed fields and extracted locations.

    This does NOT group by INF - each record is processed individually.
    Uses data already enriched by transform.py (mention_normalized, cycle, etc.).
    Uses paysage geoloc for etablissement locations (ignores geolocalisations).
    """
    details = data.get('formation_details')

    # Location collector for this record
    location_collector = LocationCollector()

    transformed_etablissements = data.get('etablissements', [])
    etablissements = [build_etablissement(d, location_collector) for d in transformed_etablissements]



    # Transform etapes with location references
    etapes: List[Etape] = []
    if details and details.get('etapes_details'):
        for e in details['etapes_details']:
            current = transform_etape(e, location_collector)
            etapes.append(current)

    # Transform parcours
    parcours: List[Parcours] = []
    if details and details.get('parcours_diplomants_full'):
        for p in details['parcours_diplomants_full']:
            current = transform_parcours(p)
            parcours.append(current)

    # Build formation document
    formation: FormationFormatted = {
        'inf': data.get('inf', ''),
        'label': data.get('intitule_officiel', ''),
        'mentionNormalized': data.get('mention_normalized', ''),
        'mentionId': data.get('mention_id', ''),
        'cycle': data.get('cycle', ''),
        'accreditation': {
            'startDate': data.get('date_debut_accreditation_min'),
            'endDate': data.get('date_fin_accreditation_max'),
            'endYears': data.get('dates_fin_reconnaissance'),
            'gradeEndDate': data.get('date_fin_grade'),
            'visaEndDate': data.get('date_fin_visa')
        },
        'diploma': {
            'code': data.get('code_type_diplome', ''),
            'type': data.get('libelle_type_diplome', ''),
            'category': data.get('categorie_type_diplome', ''),
            'order': data.get('ordre_type_diplome')
        },
        'domains': data.get('domaines'),
        'codeSise': data.get('code_sise'),
        'codeSiseValid': data.get('code_sise_valid'),
        'codeSiseInvalid': data.get('code_sise_invalid'),
        'rncp': data.get('num_rncp'),
        'qualificationLevel': details.get('niveau_qualification') if details else None,
        'teachingModalities': data.get('modalites_enseignement'),
        'healthCycle': data.get('cycle_sante'),
        'healthSpecialty': data.get('specialite_sante'),
        'engineeringSpecialties': data.get('specialites_cti'),
        'butType': data.get('type_parcours_but'),
        'butSpecialtySigle': data.get('sigle_specialite_but'),
        'disciplinarySector': data.get('secteur_disciplinaire_sise'),
        'keywords': data.get('mots_cles'),
        'etablissements': etablissements,
        'parcours': parcours,
        'etapes': etapes,
        'locations': location_collector.get_all(),
        'collectionId': data.get('collectionId'),
        'recordId': data.get('recordId'),
        'bucketId': data.get('bucketId'),
        'sourceId': data.get('identifiant_source'),
        'hasRncpInfos': data.get('avec_rncp_infos', False),
        'rncpInfos': rename_rncp_infos(data.get('rncp_infos')),
        'hasRomeInfos': data.get('avec_rome_infos', False),
        'romeInfos': rename_rome_infos(data.get('rome_infos')),
        'hasSiseInfos': data.get('avec_sise_infos', False),
        'siseInfos': data.get('sise_infos')
    }

    return formation


# ============================================================================
# MAIN FUNCTIONS
# ============================================================================


def format_transformed_data(raw_data_suffix: str = 'latest', dry_run: bool = False, output_path: Optional[str] = None) -> List[FormationFormatted]:
    logger.debug('>>>>>>>>>> TRANSFORM FORMAT >>>>>>>>>>')
    logger.debug(f'Starting FRESQ data formatting from {raw_data_suffix} (dry_run={dry_run})')

    global fresq_enriched
    if fresq_enriched is None:
        fresq_enriched = get_transformed_data(raw_data_suffix)
    if fresq_enriched is None:
        logger.error('Failed to load transformed data')
        return []

    logger.debug(f'Loaded {len(fresq_enriched)} raw records')
    logger.debug('Formatting records...')
    fresq_formatted: List[FormationFormatted] = []

    for idx, record in enumerate(fresq_enriched):
        if (idx + 1) % 2000 == 0:
            logger.debug(f'Processing record {idx + 1}/{len(fresq_enriched)}')

        formatted = format_record(record)
        fresq_formatted.append(formatted)

    logger.debug(f'Formatted {len(fresq_formatted)} records')

    # Save to file
    formatted_data_filename = get_formatted_data_filename(raw_data_suffix)
    os.system(f'rm -rf {formatted_data_filename}')
    to_jsonl(fresq_formatted, formatted_data_filename)
    logger.debug(f'Saved to {formatted_data_filename}')

    # Save to custom output path if provided
    if output_path:
        to_jsonl(fresq_formatted, output_path)
        logger.debug(f'Saved to custom path: {output_path}')

    if not dry_run:
        # Upload to storage
        upload_object('fresq', formatted_data_filename, formatted_data_filename)
        # Save logs
        save_logs()
    else:
        logger.debug('Skipping upload (dry_run=True)')

    return fresq_formatted


def get_formatted_data(raw_data_suffix: str = 'latest') -> List[FormationFormatted]:
    """Get formatted data from storage."""
    import jsonlines

    formatted_data_filename = get_formatted_data_filename(raw_data_suffix)
    download_object('fresq', formatted_data_filename, formatted_data_filename)

    with jsonlines.open(formatted_data_filename) as reader:
        return [elt for elt in reader]
