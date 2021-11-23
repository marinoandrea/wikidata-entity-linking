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
from src.linking import choose_entity_candidate, generate_entity_candidates
from src.parsing import extract_entities, extract_text_from_html
from src.warc import extract_metadata_from_warc, stream_records_from_warc

DAEMON_SLEEP_TIME_S: float = 2


def process_record(record: str):
    """
    The main data pipeline. Runs for every record in a WARC archive.
    This function is meant to be the target of a sub-process.

    Parameters
    ----------
    - record `str`
    The WARC record, including both metadata and HTML.
    """
    try:
        warc_metadata = extract_metadata_from_warc(record)
    # we are handling the case where the record is an empty string
    except ValueError:
        logging.error("warc record does not have a trec ID")
        return

    shared_dict[warc_metadata] = {
        "mappings": [],
        "is_done": False,
        "is_flushed": False
    }

    text = extract_text_from_html(record)
    named_entities = extract_entities(text)

    # free some memory
    del text

    # NOTE(andrea): this number of threads is arbitrary
    # it does not have to be tight to the core count
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

    shared_dict[warc_metadata] = {
        "mappings": [
            EntityMapping(named_entity=ent, entity_url=cand.id)
            for (ent, cand) in zip(named_entities, entity_candidates)
            if cand is not None],
        "is_done": True,
        "is_flushed": False
    }

    t_pool.close()
    t_pool.join()


def run_flush_daemon():
    """
    I/O specialized daemon which flushes linked entities to console when
    the child processes flag a record as being processed (see `WARCJobInformation.is_done`).
    """
    while True:
        time.sleep(DAEMON_SLEEP_TIME_S)

        warc_to_delete: List[WARCRecordMetadata] = []
        for warc in shared_dict.keys():
            if not shared_dict[warc]['is_done'] or shared_dict[warc]['is_flushed']:
                continue
            for mapping in shared_dict[warc]['mappings']:
                print(
                    f'{warc.trec_id}\t{mapping.named_entity.name}\t{mapping.entity_url}')
            # workaround for DictProxy update not working on nested fields
            _temp_job_info = shared_dict[warc]
            _temp_job_info['is_flushed'] = True
            shared_dict[warc] = _temp_job_info
            warc_to_delete.append(warc)

        for warc in warc_to_delete:
            shared_dict.pop(warc)


def init():
    """
    Initializes the program and the utilities.
    """
    # we use logging only in 'development' mode
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s]: %(message)s',
        level=logging.DEBUG if os.getenv('ENV') == 'development' else logging.CRITICAL)


def main():
    archive_path = parse_cl_args()

    init()
    logging.info('initialization completed')

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
        time.sleep(DAEMON_SLEEP_TIME_S)

    logging.info('all jobs terminated successfully')
    logging.info('exiting')


if __name__ == '__main__':
    main()
