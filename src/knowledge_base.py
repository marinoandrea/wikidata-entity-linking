import multiprocessing as mp
import os
from typing import Optional, Set, Tuple

import trident

from src.interfaces import CandidateNamedEntity, EntityLabel
from src.utils import cached

KB_PATH: str = os.getenv(
    'KB_PATH', "assets/wikidata-20200203-truthy-uri-tridentdb")

trident_db = trident.Db(KB_PATH)
trident_lock = mp.Lock()


@cached
def fetch_id(term: str) -> Optional[int]:
    """
    Cached version of `trident.Db.lookup_id`.

    Parameters
    ----------
    term `str`
    The wikidata URI.

    Returns
    -------
    `Optional[int]` The trident internal ID or None.
    """
    return trident_db.lookup_id(term)


@cached
def fetch_attributes(entity_id: int) -> Set[Tuple[int, int]]:
    """
    Cached version of `trident.Db.po`.

    Parameters
    ----------
    entity_id `int`
    The Trident internal ID.

    Returns
    -------
    'Set[Tuple[int, int]]'
    Set of tuples in the form (predicate, object).
    """
    return set(trident_db.po(entity_id))


@cached
def score_candidate(label: EntityLabel, candidate: CandidateNamedEntity) -> float:
    """
    This function scores a named entity candidate based on compliance with
    a specific NER label.

    Parameters
    ----------
    label `EntityLabel`
    The label to compare the candidates with.

    candidate `CandidateNamedEntity`
    The list of named entity candidates.

    Returns
    -------
    `float` The compliance score.
    """
    trident_lock.acquire(block=True)
    score = {
        EntityLabel.PERSON: score_person,
        EntityLabel.NORP: score_norp,
        EntityLabel.FAC: score_fac,
        EntityLabel.ORG: score_org,
        EntityLabel.GPE: score_gpe,
        EntityLabel.LOC: score_loc,
        EntityLabel.PRODUCT: score_product,
        EntityLabel.EVENT: score_event,
        EntityLabel.WORK_OF_ART: score_work_of_art,
        EntityLabel.LAW: score_law,
        EntityLabel.LANGUAGE: score_language,
        EntityLabel.DATE: score_date,
    }[label](fetch_id(candidate.id))
    trident_lock.release()
    return score


# pre-fetch utility trident ids


# instance of
PREDICATE_ID_P31 = fetch_id('<http://www.wikidata.org/prop/direct/P31>')
# subclass of
PREDICATE_ID_P279 = fetch_id('<http://www.wikidata.org/prop/direct/P279>')
# named after
PREDICATE_ID_P138 = fetch_id('<http://www.wikidata.org/prop/direct/P138>')
# followed by
PREDICATE_ID_P156 = fetch_id('<http://www.wikidata.org/prop/direct/P156>')
# day of week
PREDICATE_ID_P2894 = fetch_id('<http://www.wikidata.org/prop/direct/P2894>')
#  title i.e. movie
PREDICATE_ID_P1476 = fetch_id('<http://www.wikidata.org/prop/direct/P1476>')
# genre type of film / movie / kind of music
PREDICATE_ID_P136 = fetch_id('<http://www.wikidata.org/prop/direct/P136>')
# made from materials
PREDICATE_ID_P186 = fetch_id('<http://www.wikidata.org/prop/direct/P186>')
# cars - industry
PREDICATE_ID_P452 = fetch_id('<http://www.wikidata.org/prop/direct/P452>')
# start time
PREDICATE_ID_P580 = fetch_id('<http://www.wikidata.org/prop/direct/P580>')
# end time
PREDICATE_ID_P582 = fetch_id('<http://www.wikidata.org/prop/direct/P582>')
# lowest atmospheric pressure
PREDICATE_ID_P2532 = fetch_id('<http://www.wikidata.org/prop/direct/P2532>')
# part of the series
PREDICATE_ID_P179 = fetch_id('<http://www.wikidata.org/prop/direct/P179>')

# predicates for FAC
# Location
PREDICATE_ID_P276 = fetch_id('<http://www.wikidata.org/prop/direct/P276>')
# Coordinate location
PREDICATE_ID_P625 = fetch_id('<http://www.wikidata.org/prop/direct/P625>')
# Architectural style
PREDICATE_ID_P149 = fetch_id('<http://www.wikidata.org/prop/direct/P149>')
# Maximum capacity
PREDICATE_ID_P1083 = fetch_id('<http://www.wikidata.org/prop/direct/P1083>')
# Located in the administrative territorial entity
PREDICATE_ID_P131 = fetch_id('<http://www.wikidata.org/prop/direct/P131>')
# Street address
PREDICATE_ID_P6375 = fetch_id('<http://www.wikidata.org/prop/direct/P6375>')

# predicates for organization
# Founded by
PREDICATE_ID_P112 = fetch_id('<http://www.wikidata.org/prop/direct/P112>')
# Chief executive officer
PREDICATE_ID_P169 = fetch_id('<http://www.wikidata.org/prop/direct/P169>')
# Board member
PREDICATE_ID_P3320 = fetch_id('<http://www.wikidata.org/prop/direct/P3320>')
# Motto
PREDICATE_ID_P1546 = fetch_id('<http://www.wikidata.org/prop/direct/P1546>')
# Motto text
PREDICATE_ID_P1451 = fetch_id('<http://www.wikidata.org/prop/direct/P1451>')
# Owned by
PREDICATE_ID_P127 = fetch_id('<http://www.wikidata.org/prop/direct/P127>')
# Employees
PREDICATE_ID_P1128 = fetch_id('<http://www.wikidata.org/prop/direct/P1128>')
# Product or material produced
PREDICATE_ID_P1056 = fetch_id('<http://www.wikidata.org/prop/direct/P1056>')
# Legal form
PREDICATE_ID_P1454 = fetch_id('<http://www.wikidata.org/prop/direct/P1454>')
# Inception
PREDICATE_ID_P571 = fetch_id('<http://www.wikidata.org/prop/direct/P571>')
# Total revenue
PREDICATE_ID_P2139 = fetch_id('<http://www.wikidata.org/prop/direct/P2139>')
# Director/manager
PREDICATE_ID_P1037 = fetch_id('<http://www.wikidata.org/prop/direct/P1037>')

# predicates for language
# native label
PREDICATE_ID_P1705 = fetch_id('<http://www.wikidata.org/prop/direct/P1705>')
# country
PREDICATE_ID_P17 = fetch_id('<http://www.wikidata.org/prop/direct/P17>')
# has part
PREDICATE_ID_P527 = fetch_id('<http://www.wikidata.org/prop/direct/P527>')


def score_person(entity_id: int) -> float:
    if not trident_db.exists(entity_id, PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q5>')):
        return 0
    attributes = fetch_attributes(entity_id)
    # prioritize entities with more annotations
    return len(attributes)


def score_norp(entity_id: int) -> float:
    template_attributes = {
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q41710>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q33829>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q22947>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q6266>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q4392985>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q16334295>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q17573152>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/P140>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q7140620>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q844569>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q11499147>')),
    }
    attributes = fetch_attributes(entity_id)
    return len(attributes & template_attributes) / len(template_attributes)


def score_fac(entity_id: int) -> float:
    attributes = fetch_attributes(entity_id)
    if (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q41176>')) not in attributes:
        return 0
    return len(attributes)

def score_org(entity_id: int) -> float:
    attributes = fetch_attributes(entity_id)
    if (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q4830453>')) not in attributes:
        return 0
    return len(attributes)


def score_gpe(entity_id: int) -> float:
    template_attributes = {
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q3624078>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q619610>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q179164>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q6256>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q515>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q1549591>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q208511>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q2264924>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q208511>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q486972>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q532>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q7930989>'))
    }
    attributes = fetch_attributes(entity_id)
    return len(attributes & template_attributes) / len(template_attributes)


def score_loc(entity_id: int) -> float:
    attributes = fetch_attributes(entity_id)
    if (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q2221906>')) not in attributes:
        return 0
    return len(attributes)


def score_product(entity_id: int) -> float:
    # instance of food, cars, objects
    template_attributes = {
        # food
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q2095>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q2095>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q746549>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q17062980>')),
        # (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q84431525>')),
        # cars
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q10429667>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q786820>')),
        (PREDICATE_ID_P452, fetch_id('<http://www.wikidata.org/entity/Q190117>')),
        # objects
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q2578402>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q811367>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q39546>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q1183543>')),

    }
    attributes = fetch_attributes(entity_id)
    return len(attributes & template_attributes) / len(template_attributes)


def score_event(entity_id: int) -> float:
    # instance of Wars, rebellions, battles, sport, hurricanes
    template_attributes = {
        # wars
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q103495>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q11514315>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q198>')),

        # rebellions
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q124734>')),

        # battles: includes part of & location & point in time
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q178561>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q1261499>')),

        # sport
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q18608583>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q44637051>')),

        # hurricane Saffir–Simpson classification category 1 - 5
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q63100559>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q63100584>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q63100595>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q63100601>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q63100611>')),
        (PREDICATE_ID_P179, fetch_id('<http://www.wikidata.org/entity/Q205801>')),


    }
    attributes = fetch_attributes(entity_id)
    return len(attributes & template_attributes) / len(template_attributes)


def score_work_of_art(entity_id: int) -> float:
    # movies, songs, books, novels, sculptures --> title p1476 & genre p136
    template_attributes = {
        # film
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q11424>')),
        # single
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q134556>')),
        # song
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q7366>')),
        # sculpture
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q860861>')),
        # archaeological findings
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q10855061>')),
        # literary work
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q7725634>')),

    }
    attributes = fetch_attributes(entity_id)
    return len(attributes & template_attributes) / len(template_attributes)


def score_law(entity_id: int) -> float:
    attributes = fetch_attributes(entity_id)
    if (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q7748>')) not in attributes:
        return 0
    return len(attributes)


def score_language(entity_id: int) -> float:
    template_attributes = {
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q34770>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q1288568>')),
    }
    attributes = fetch_attributes(entity_id)
    return len(attributes & template_attributes) / len(template_attributes)


def score_date(entity_id: int) -> float:
    template_attributes = {
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q14795564>')),
        # days of the week
        (PREDICATE_ID_P2894, fetch_id('<http://www.wikidata.org/entity/Q132>')),
        (PREDICATE_ID_P2894, fetch_id('<http://www.wikidata.org/entity/Q105>')),
        (PREDICATE_ID_P2894, fetch_id('<http://www.wikidata.org/entity/Q127>')),
        (PREDICATE_ID_P2894, fetch_id('<http://www.wikidata.org/entity/Q128>')),
        (PREDICATE_ID_P2894, fetch_id('<http://www.wikidata.org/entity/Q129>')),
        (PREDICATE_ID_P2894, fetch_id('<http://www.wikidata.org/entity/Q130>')),
        (PREDICATE_ID_P2894, fetch_id('<http://www.wikidata.org/entity/Q131>')),
    }
    attributes = fetch_attributes(entity_id)
    return len(attributes & template_attributes) / len(template_attributes)
