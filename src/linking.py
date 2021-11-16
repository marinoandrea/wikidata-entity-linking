from typing import List, Optional, Tuple

import elasticsearch as es
import trident

from src.interfaces import NamedEntity
from src.utils import cached


@cached
def generate_entity_candidates(es_client: es.Elasticsearch, entity: NamedEntity) -> List[str]:
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
        response = es_client.search(index="wikidata_en", body={
            "query": {
                "query_string": {
                    "query": entity.name,
                    "fields": ["schema_name", "schema_description"]
                },
            }
        })
        return [hit['_id'] for hit in response['hits']['hits']] if response else []
    except Exception as e:
        return []


def choose_entity_candidate(trident_db: trident.Db, entity_with_candidates: Tuple[NamedEntity, List[str]]) -> Optional[str]:
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

    # FIXME(andrea): right now we just naively get the last id in the list
    # this is just for testing purposes
    return candidates[-1]
