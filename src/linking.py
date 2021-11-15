import json
from pprint import pprint
from typing import List, Optional

import elasticsearch as es
from elasticsearch import exceptions

from src.interfaces import NamedEntity
from src.utils import cached


@cached
def generate_entity_candidates(es_client: es.Elasticsearch, entity: NamedEntity) -> Optional[str]:
    """

    """
    try:
        response = es_client.search(index="wikidata_en", body={
            "query": {
                "query_string": {
                    "query": entity.name,
                    "fields": ["schema_name"]
                },
            }
        })
    except Exception:
        return None

    # FIXME(andrea): we are just returning the 'best' candidate,
    # but this function should return the entire list in the future.
    max_score = 0
    candidate: Optional[str] = None
    if response:
        for hit in response['hits']['hits']:
            max_score = max(hit['_score'], max_score)
            if max_score == hit['_score']:
                candidate = hit['_id']

    return candidate


def rank_entity_candidates(candidates: List[str]) -> List[str]:
    """

    """
    raise NotImplementedError()
