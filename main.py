import logging
import multiprocessing as mp
import os
import time
from functools import partial
from multiprocessing.pool import ThreadPool
from typing import Dict, List

import elasticsearch as es

from src.cli import parse_cl_args
from src.globals import shared_dict
from src.interfaces import (CandidateNamedEntity, EntityMapping, NamedEntity,
                            WARCRecordMetadata)
from src.io import run_flush_daemon
from src.linking import choose_entity_candidate, generate_entity_candidates
from src.parsing import extract_entities, extract_text_from_html
from src.warc import extract_metadata_from_warc, stream_records_from_warc


def process_record(record: str):
    """
    The main data pipeline. Runs for every record in a WARC archive.
    This function is meant to be the target of a sub-process.

    Parameters
    ----------
    - record `str`
    The WARC record string, including both metadata and HTML.
    """
    try:
        warc_metadata = extract_metadata_from_warc(record)
    # we are handling the case where the record is an empty string
    except ValueError:
        logging.error("warc record does not have a trec ID")
        return

    # init job information in the shared dict
    # in order for the i/o daemon to check upon
    # the status of this page
    shared_dict[warc_metadata] = {
        "mappings": [],
        "is_done": False,
        "is_flushed": False
    }

    text = extract_text_from_html(record)
    named_entities = extract_entities(text)

    # free some memory
    del text

    t_pool = ThreadPool()

    # the es client is thread safe, we spawn one for each child
    # process due to complication with 'fork' mentioned in the es docs:
    # https://elasticsearch-py.readthedocs.io/en/v7.15.2/api.html#elasticsearch
    es_client = es.Elasticsearch(maxsize=mp.cpu_count())

    entity_candidates_list = t_pool.map(
        partial(generate_entity_candidates, es_client), named_entities)

    candidate_cache: Dict[NamedEntity, CandidateNamedEntity] = {}

    entity_candidates = t_pool.map(
        partial(choose_entity_candidate, candidate_cache),
        zip(named_entities, entity_candidates_list))

    t_pool.close()
    t_pool.join()

    shared_dict[warc_metadata] = {
        "mappings": [
            EntityMapping(named_entity=ent, entity_url=cand.id)
            for (ent, cand) in zip(named_entities, entity_candidates)
            if cand is not None],
        "is_done": True,
        "is_flushed": False
    }


def main():
    archive_path = parse_cl_args()

    # in production, we only log critical errors
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s]: %(message)s',
        level=logging.DEBUG if os.getenv('ENV') == 'development' else logging.CRITICAL)

    # start daemon which flushes linked entities to file periodically
    mp.Process(
        target=run_flush_daemon,
        daemon=True
    ).start()

    logging.info(f'processing archive \'{archive_path}\'')
    process_pool = mp.Pool()
    process_pool.map(process_record, stream_records_from_warc(archive_path))
    process_pool.close()
    process_pool.join()
    logging.info('processing completed')

    # waiting for all the entities to be flushed to file
    logging.info('waiting for I/O to finish')
    while not all(p_info['is_flushed'] for p_info in shared_dict.values()):
        time.sleep(1)

    logging.info('all jobs terminated successfully')
    logging.info('exiting')


if __name__ == '__main__':
    main()
