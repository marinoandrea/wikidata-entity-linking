import multiprocessing as mp
import os
from typing import Set, Tuple

import trident

from src.interfaces import CandidateNamedEntity, EntityLabel
from src.utils import cached

KB_PATH: str = os.getenv(
    'KB_PATH', "assets/wikidata-20200203-truthy-uri-tridentdb")

trident_db = trident.Db(KB_PATH)
trident_lock = mp.Lock()


@cached
def fetch_id(term: str) -> int:
    out = trident_db.lookup_id(term)
    if out is None:
        raise RuntimeError(f"'{term}' is not a valid wikidata URI.")
    return out


@cached
def fetch_attributes(entity_id: int) -> Set[Tuple[int, int]]:
    return set(trident_db.po(entity_id))

# pre-fetch utility trident ids


# instance of
PREDICATE_ID_P31 = fetch_id('<http://www.wikidata.org/prop/direct/P31>')
# subclass of
PREDICATE_ID_P279 = fetch_id('<http://www.wikidata.org/prop/direct/P279>')
# named after
PREDICATE_ID_P138 = fetch_id('<http://www.wikidata.org/prop/direct/P138>')

# if they have P1476 & P136 -> work of art but -> check presence 
#  title i.e. movie 
PREDICATE_ID_P1476 = fetch_id('<http://www.wikidata.org/prop/direct/P1476>')
# genre type of film / movie / kind of music 
PREDICATE_ID_P136 = fetch_id('<http://www.wikidata.org/prop/direct/P136>')
# made from materials 
PREDICATE_ID_P186 = fetch_id('<http://www.wikidata.org/prop/direct/P186>')
# cars - industry
PREDICATE_ID_P452 = fetch_id('<http://www.wikidata.org/prop/direct/P452>')
# if attributes: start time (P580) & end time (P582) -> events
PREDICATE_ID_P580 = fetch_id('<http://www.wikidata.org/prop/direct/P580>')
PREDICATE_ID_P582 = fetch_id('<http://www.wikidata.org/prop/direct/P580>')
# if attribute lowest atmospheric pressure (P2532) or part of the series (P179) -> Hurricane 
PREDICATE_ID_P2532 = fetch_id('<http://www.wikidata.org/prop/direct/P2532>')
PREDICATE_ID_P179 = fetch_id('<http://www.wikidata.org/prop/direct/P179>')


def score_person(entity_id: int) -> float:
    return 1 if trident_db.exists(
        entity_id,
        PREDICATE_ID_P31,
        fetch_id('<http://www.wikidata.org/entity/Q5>')) else 0


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
    raise NotImplementedError()


def score_org(entity_id: int) -> float:
    raise NotImplementedError()


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
    raise NotImplementedError()


def score_product(entity_id: int) -> float:
    # instance of food, cars, objects 
    template_attributes = {
        # food 
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q2095>')),
        (PREDICATE_ID_P279, fetch_id('<http://www.wikidata.org/entity/Q2095>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q746549>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q17062980>')),
        (PREDICATE_ID_P31, fetch_id('<http://www.wikidata.org/entity/Q84431525>')),
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
    raise NotImplementedError()


def score_language(entity_id: int) -> float:
    raise NotImplementedError()


def score_date(entity_id: int) -> float:
    raise NotImplementedError()


@cached
def score_candidate(label: EntityLabel, candidate: CandidateNamedEntity) -> float:
    """
    This function scores a named entity candidate based on compliance with
    a specific NER label.

    Parameters
    ----------
    candidate `CandidateNamedEntity`
    The list of named entity candidates.

    label `EntityLabel`
    The label to compare the candidates with.

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
