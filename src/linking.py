import json
from pprint import pprint
from time import time
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


SPARQL_RESULT_VAR_NAME: str = 'record'


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
                        "fields": ["schema_name", "schema_description"]
                    },
                }
            })
        return {hit['_id'] for hit in response['hits']['hits']} if response else set()
    except Exception as e:
        return set()


candidate_cache: Dict[NamedEntity, str] = {}


def choose_entity_candidate(trident_db: trident.Db, entity_with_candidates: Tuple[NamedEntity, Set[str]]) -> Optional[str]:
    """
    Given a name entity and a list of candidates it uses the Trident db
    to perform SPARQL queris tailored to the specific entity category and
    pick the most likely candidate.

    Parameters
    ----------
    trident_db: `trident.Db`
    Trident db client instance.

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

    query = f'''
    {SPARQL_QUERY_PREFIX}
    SELECT ?record
    WHERE {{
    '''

    if entity.label == 'PERSON':
        query += '?record wdp:P31 wde:Q5.'          # human

    elif entity.label == 'NORP':
        query += '?record wdp:P31 wde:Q16334295.'   # human group

    elif entity.label == 'FAC':
        query += '?record wdp:P31 wde:Q41176.'      # building

    elif entity.label == 'ORG':
        query += '?record wdp:P31 wde:Q43229.'      # organization

    elif entity.label == 'GPE':
        query += '?record wdp:P31 wde:Q16562419.'   # political entity

    elif entity.label == 'LOC':
        query += '?record wdp:P31 wde:Q27096213.'   # geographic entity

    elif entity.label == 'PRODUCT':
        query += '?record wdp:P31 wde:Q2424752.'    # product

    elif entity.label == 'EVENT':
        query += '?record wdp:P31 wde:Q1190554.'    # occurrence

    elif entity.label == 'WORK_OF_ART':
        query += '?record wdp:P31 wde:Q838948.'     # work of art

    elif entity.label == 'LAW':
        query += '?record wdp:P31 wde:Q1151067.'    # rule (law)

    elif entity.label == 'LANGUAGE':
        query += '?record wdp:P31 wde:Q34770.'      # language

    elif entity.label == 'DATE':
        query += '?record wdp:P31 wde:Q205892.'     # calendar date

    elif entity.label == 'TIME':
        query += '?record wdp:P31 wde:Q2199864.'    # duration

    elif entity.label == 'PERCENT':
        query += '?record wdp:P31 wde:Q11229.'      # percent

    elif entity.label == 'MONEY':
        query += '?record wdp:P31 wde:Q30242023.'   # money amount

    elif entity.label == 'QUANTITY':
        query += '?record wdp:P31 wde:Q107715.'     # physical quantity

    elif entity.label == 'ORDINAL':
        query += '?record wdp:P31 wde:Q191780.'     # ordinal number

    elif entity.label == 'CARDINAL':
        query += '?record wdp:P31 wde:Q163875.'     # cardinal number

    else:
        return candidates.pop()

    query += f'''
        FILTER ( {' || '.join(f"?record = wde:{c.replace('>', '').split('/')[-1]}" for c in candidates)} )
    }}
    LIMIT 10
    '''

    results = trident_db.sparql(query)
    json_results = json.loads(results)

    filtered_candidates: List[str] = []
    for result in json_results['results']['bindings']:
        try:
            filtered_candidates.append(
                next(c for c in candidates if result[SPARQL_RESULT_VAR_NAME]['value']))
        except StopIteration:
            continue

    # NOTE(andrea): if we don't find anything with trident we just naively
    # return one of the initial elasticsearch candidates
    if len(filtered_candidates) == 0:
        return candidates.pop()

    return filtered_candidates[0]
