import json
from typing import Dict, List, Optional, Set, Tuple

import elasticsearch as es
import trident

from src.interfaces import NamedEntity
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
def generate_entity_candidates(es_client: es.Elasticsearch, entity: NamedEntity) -> Set[str]:
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
    `List[str]` List of candidates in the form of wikidata doc ids.
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
        return {hit['_id'] for hit in response['hits']['hits']} if response else set()
    except Exception as e:
        return set()


def choose_entity_candidate(
    trident_db: trident.Db,
    candidate_cache: Dict[NamedEntity, str],
    entity_with_candidates: Tuple[NamedEntity, Set[str]]
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

    filtered_candidates: List[str] = []

    for candidate in candidates:

        id = candidate\
            .replace('>', '')\
            .split('/')[-1]

        superclass = LABEL_SUPERCLASS_LOOKUP_TABLE.get(entity.label, None)

        if superclass is None:
            continue

        query = f'''
        {SPARQL_QUERY_PREFIX}
        select *
        where {{ wde:{id} wdp:P31 wde:{superclass} }}
        limit 10
        '''

        results = trident_db.sparql(query)
        json_results = json.loads(results)

        if json_results is not None and 'nresults' in json_results['stats'] and int(json_results['stats']['nresults']) > 0:
            filtered_candidates.append(candidate)

    if len(filtered_candidates) == 0:
        # NOTE(andrea): if we don't find anything with trident we just naively
        # return one of the initial elasticsearch candidates
        candidate_cache[entity] = next(c for c in candidates)
    else:
        # TODO(andrea): we should have a ranking strategy for
        # selecting one of the filtered candidates
        candidate_cache[entity] = filtered_candidates[0]

    return candidate_cache[entity]
