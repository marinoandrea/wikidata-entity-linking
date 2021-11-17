import logging
import multiprocessing as mp
import os
import time
from functools import partial
from multiprocessing.pool import ThreadPool
from typing import Dict, List

import elasticsearch as es
import trident

from src.cli import parse_cl_args
from src.interfaces import (EntityMapping, NamedEntity, WARCJobInformation,
                            WARCRecordMetadata)
from src.linking import choose_entity_candidate, generate_entity_candidates
from src.parsing import extract_entities, extract_text_from_html
from src.warc import extract_metadata_from_warc, stream_records_from_warc

DAEMON_SLEEP_TIME_S: float = 2
KB_PATH: str = os.getenv(
    'KB_PATH', "assets/wikidata-20200203-truthy-uri-tridentdb")


def process_record(output_dict: Dict[WARCRecordMetadata, WARCJobInformation], record: str):
    """
    The main data pipeline. Runs for every record in a WARC archive.
    This function is meant to be the target of a sub-process and takes
    a dict proxy provided by `multiprocessing.Manager` in order to 
    synchronize operations with the flush daemon.

    Parameters
    ----------
    - output_dict `Dict[WARCRecordMetadata, WARCJobInformation]`
    Instance of `multiprocessing.Manager.dict()`. It is populated
    with job-relevant information.

    - record `str`
    The WARC record, including both metadata and HTML.
    """
    if record is None:
        return

    try:
        warc_metadata = extract_metadata_from_warc(record)
    # we are handling the case where the record is an empty string
    except ValueError:
        logging.error("warc record does not have a trec ID")
        return

    output_dict[warc_metadata] = {
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
    t_pool = ThreadPool(processes=mp.cpu_count())

    # the es client is thread safe, we spawn one for each child
    # process due to complication with 'fork' mentioned in the es docs:
    # https://elasticsearch-py.readthedocs.io/en/v7.15.2/api.html#elasticsearch
    es_client = es.Elasticsearch(maxsize=mp.cpu_count())

    entity_candidates_list = t_pool.map(
        partial(generate_entity_candidates, es_client), named_entities)

    # the trident db is not typed nor we have access to a public repo
    # so we have no assurance about thread safety, but since we only
    # do read operations it should be fine
    trident_db = trident.Db(KB_PATH)

    candidate_cache: Dict[NamedEntity, str] = {}

    entity_candidates = t_pool.map(
        partial(choose_entity_candidate, trident_db, candidate_cache), zip(named_entities, entity_candidates_list))

    output_dict[warc_metadata] = {
        "mappings": [EntityMapping(named_entity=ent, entity_url=cand) for (ent, cand) in zip(named_entities, entity_candidates)],
        "is_done": True,
        "is_flushed": False
    }


def process_archive(output_dict: Dict[WARCRecordMetadata, WARCJobInformation], warc_path: str):
    """
    A utilty function to map WARC archives into separate processes
    which will initialize a `multiprocessing.Pool` that maps every record
    to the main processing function (see `process_record`).

    Parameters
    ----------
    - output_dict `Dict[WARCRecordMetadata, WARCJobInformation]`
    Instance of `multiprocessing.Manager.dict()`. It is populated
    with job-relevant information.

    - warc_path `str`
    The WARC archive path.
    """
    pool = mp.Pool(processes=mp.cpu_count())
    pool.map(partial(process_record, output_dict),
             stream_records_from_warc(warc_path))
    pool.close()
    pool.join()


def flush_daemon(output_dict: Dict[WARCRecordMetadata, WARCJobInformation]):
    """
    I/O specialized daemon which flushes linked entities to file when
    the child processes flag a record as being processed (see `WARCJobInformation.is_done`). 

    Parameters
    ----------
    - output_dict `Dict[WARCRecordMetadata, WARCJobInformation]`
    Instance of `multiprocessing.Manager.dict()`. It is populated
    with job-relevant information.

    - output_path `str`
    The output TSV file path.
    """
    while True:
        time.sleep(DAEMON_SLEEP_TIME_S)

        warc_to_delete: List[WARCRecordMetadata] = []
        for warc in output_dict.keys():
            if not output_dict[warc]['is_done'] or output_dict[warc]['is_flushed']:
                continue
            for mapping in output_dict[warc]['mappings']:
                print(
                    f'{warc.trec_id}\t{mapping.named_entity.name}\t{mapping.entity_url}')
            # workaround for DictProxy update not working on nested fields
            _temp_job_info = output_dict[warc]
            _temp_job_info['is_flushed'] = True
            output_dict[warc] = _temp_job_info
            warc_to_delete.append(warc)

        for warc in warc_to_delete:
            output_dict.pop(warc)


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

    # multiprocessing setup
    manager = mp.Manager()
    shared_dict = manager.dict()

    # start daemon which flushes linked entities to file periodically
    mp.Process(
        target=flush_daemon,
        args=(shared_dict, ),
        daemon=True
    ).start()

    process_archive(shared_dict, archive_path)
    logging.info('processing completed')

    # waiting for all the entities to be flushed to file
    logging.info('waiting for I/O to finish')
    while not all(p_info['is_flushed'] for p_info in shared_dict.values()):
        time.sleep(DAEMON_SLEEP_TIME_S)

    logging.info('all jobs terminated successfully')
    logging.info('exiting')


if __name__ == '__main__':
    main()
