import logging
import multiprocessing as mp
import time
from functools import partial
from typing import Dict

from nltk import chunk

from src.cli import parse_cl_args
from src.interfaces import WARCJobInformation, WARCRecordMetadata
from src.parsing import (extract_entities, extract_text_from_html,
                         init_parsing, tokenize_and_tag_raw_text)
from src.warc import extract_metadata_from_warc, stream_records_from_warc

DAEMON_SLEEP_TIME_S: float = 0.250


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
    tagged_tokens = tokenize_and_tag_raw_text(text)
    named_entities = extract_entities(tagged_tokens)

    # TODO: do something with our named entities!
    # ....

    # workaround for DictProxy update not working on nested fields
    _temp_job_info = output_dict[warc_metadata]
    _temp_job_info['is_done'] = True
    output_dict[warc_metadata] = _temp_job_info


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


def flush_daemon(output_dict: Dict[WARCRecordMetadata, WARCJobInformation], output_path: str):
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
        with open(output_path, 'w+') as f:
            for warc in output_dict.keys():
                if not output_dict[warc]['is_done']:
                    continue
                for mapping in output_dict[warc]['mappings']:
                    f.write(
                        f'{warc.trec_id}\t{mapping.named_entity.name}\t{mapping.entity_url}\n')
                # workaround for DictProxy update not working on nested fields
                _temp_job_info = output_dict[warc]
                _temp_job_info['is_flushed'] = True
                output_dict[warc] = _temp_job_info


def init():
    """
    Initializes the program and the utilities.
    """
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s]: %(message)s', level=logging.INFO)
    logging.info('initializing NLTK dependencies')
    init_parsing()


def main():
    archives, output_path = parse_cl_args()

    init()
    logging.info('initialization completed')

    # multiprocessing setup
    manager = mp.Manager()
    shared_dict = manager.dict()
    processes: list[mp.Process] = []

    # start daemon which flushes linked entities to file periodically
    mp.Process(
        target=flush_daemon,
        args=(shared_dict, output_path),
        daemon=True
    ).start()

    for warc in archives:
        logging.info(f'starting job for archive \'{warc}\'')
        process = mp.Process(target=process_archive, args=(shared_dict, warc))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()

    logging.info('processing completed')

    # waiting for all the entities to be flushed to file
    logging.info('waiting for I/O to finish')
    while not all(p_info['is_flushed'] for p_info in shared_dict.values()):
        time.sleep(DAEMON_SLEEP_TIME_S)

    logging.info('all jobs terminated successfully')
    logging.info('exiting')


if __name__ == '__main__':
    main()
