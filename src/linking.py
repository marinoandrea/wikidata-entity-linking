from functools import partial
from typing import Dict, List, Optional, Tuple

import elasticsearch as es

from src.interfaces import CandidateNamedEntity, NamedEntity
from src.knowledge_base import score_candidate
from src.utils import cached


@cached
def generate_entity_candidates(es_client: es.Elasticsearch, entity: NamedEntity) -> List[CandidateNamedEntity]:
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
    `List[CandidateNamedEntity]` List of candidates in the form of wikidata docs metadata.
    """
    try:
        response = es_client.search(
            size=15,
            index="wikidata_en",
            request_cache=True,
            body={"query": {"query_string": {"query": entity.name, }}})

        candidates: List[CandidateNamedEntity] = []
        for hit in response['hits']['hits']:
            description = hit['_source'].get("schema_description", "")
            # we skip disambiguation pages as we don't want to perform external network requests
            # to actually use them
            if 'Wikimedia disambiguation page' in description:
                continue

            label: str = ""
            for field in ['schema_name', 'rdfs_label', 'skos_prefLabel', 'skos_altLabel', 'wikidata_P1476']:
                if field in hit['_source']:
                    label = hit['_source'][field]
                    break

            candidates.append(CandidateNamedEntity(
                id=hit["_id"],
                es_score=hit["_score"],
                label=label,
                description=description))

        return candidates

    except es.ElasticsearchException as e:
        return []


def choose_entity_candidate(
    candidate_cache: Dict[NamedEntity, CandidateNamedEntity],
    entity_with_candidates: Tuple[NamedEntity, List[CandidateNamedEntity]]
) -> Optional[CandidateNamedEntity]:
    """
    Given a name entity and a list of candidates it uses the Trident db
    to perform SPARQL queris tailored to the specific entity category and
    pick the most likely candidate.

    This function sorts the provided canidate list in place
    in descending order based on their score of compliance with
    a specific NER label.

    Parameters
    ----------
    candidate_cache: `Dict[NamedEntity, CandidateNamedEntity]`
    Selected candidate cache per named entity.

    entity_with_candidates: `Tuple[NamedEntity, List[CandidateNamedEntity]]`
    Tuple of named entity ([0]) alongside its candidates ([1]).

    Returns
    -------
    `Optional[CandidateNamedEntity]` Highest ranked candidate id.
    """
    entity, candidates = entity_with_candidates

    if len(candidates) == 0:
        return None

    if entity in candidate_cache:
        return candidate_cache[entity]

    candidates.sort(key=partial(score_candidate, entity.label), reverse=True)

    candidate_cache[entity] = candidates[0]
    return candidate_cache[entity]


# def compute_similarity(data:  Tuple[np.ndarray, CandidateNamedEntity]):
#    """
#    Computes cosine similarity for the provided entity vector and the
#    candidate vector that this generates.
#    """
#    entity_vector, candidate = data
#    candidate_vector = spacy_nlp(
#        candidate.label + " " + candidate.description).vector
#    candidate.similarity_score = calculate_similarity(
#        entity_vector, candidate_vector)
