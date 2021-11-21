from typing import Dict, Optional, Set, Tuple

import elasticsearch as es

from src.globals import trident_queue
from src.interfaces import CandidateNamedEntity, NamedEntity, TridentQueryTask
from src.utils import cached

SPARQL_QUERY_PREFIX: str = '''
PREFIX wde: <http://www.wikidata.org/entity/>
PREFIX wdp: <http://www.wikidata.org/prop/direct/>
PREFIX wdpn: <http://www.wikidata.org/prop/direct-normalized/>
'''

LABEL_SUPERCLASS_LOOKUP_TABLE: Dict[str, str] = {
    'PERSON': 'Q5',
    'NORP': 'Q16334295',
    'FAC': 'Q41176',
    'ORG': 'Q43229',
    'GPE': 'Q16562419',
    'LOC': 'Q27096213',
    'PRODUCT': 'Q2424752',
    'EVENT': 'Q1190554',
    'WORK_OF_ART': 'Q838948',
    'LAW': 'Q1151067',
    'LANGUAGE': 'Q34770',
    'DATE': 'Q205892',
    'TIME': 'Q2199864',
    'PERCENT': 'Q11229',
    'MONEY': 'Q30242023',
    'QUANTITY': 'Q107715',
    'ORDINAL': 'Q191780',
    'CARDINAL': 'Q163875'
}


@cached
def generate_entity_candidates(es_client: es.Elasticsearch, entity: NamedEntity) -> Set[CandidateNamedEntity]:
    """
    Queries local elasticsearch instance for entity candidates based on simple
    string comparison between named entity and wikidata `doc.schema_name`.

    Parameters
    ----------
    es_client: `elasticsearch.Elasticsearch`
    Elasticsearch client instance (thread safe client).

    entity: `NamedEntity`
    Named entity extracted from text.

    Returns
    -------
    `Set[CandidateNamedEntity]` List of candidates in the form of wikidata docs metadata.
    """
    try:
        response = es_client.search(
            index="wikidata_en",
            body={
                "query": {
                    "query_string": {
                        "query": entity.name,
                        "fields": ["schema_name", "schema_label"]
                    },
                }
            })
        return {
            CandidateNamedEntity(
                id=res["_id"],
                score=res["_score"],
                label=res['_source'].get("schema_label", ""),
                description=res['_source'].get("schema_description", ""))
            for res in response['hits']['hits']
        }

    except Exception as e:
        return set()


def choose_entity_candidate(
    candidate_cache: Dict[NamedEntity, str],
    entity_with_candidates: Tuple[NamedEntity, Set[CandidateNamedEntity]]
) -> Optional[str]:
    """
    Given a name entity and a list of candidates it uses the Trident db
    to perform SPARQL queris tailored to the specific entity category and
    pick the most likely candidate.

    Parameters
    ----------
    trident_db: `trident.Db`
    Trident db client instance.

    candidate_cache: `Dict[NamedEntity, str]`
    Selected candidate cache per named entity.

    entity_with_candidates: `Tuple[NamedEntity, List[str]]`
    Tuple of named entity ([0]) alongside its candidates ids ([1]).

    Returns
    -------
    `Optional[str]` Highest ranked candidate id.
    """
    entity, candidates = entity_with_candidates

    if len(candidates) == 0:
        return None

    if entity in candidate_cache:
        return candidate_cache[entity]

    for c in candidates:
        trident_queue.put(TridentQueryTask(candidate_id=c.id))

    # TODO(andrea): actually wait for the result and do something

    candidate_cache[entity] = candidates.pop().id
    return candidate_cache[entity]
