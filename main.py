import logging
import multiprocessing as mp
import time
from typing import Dict

from src.cli import parse_cl_args
from src.interfaces import WARCJobInformation, WARCRecordMetadata
from src.parsing import (extract_metadata_from_warc, extract_text_from_html,
                         init_parsing, tokenize_and_tag_raw_text)
from src.warc import stream_records_from_warc

FLUSH_THRESHOLD: int = 500
DAEMON_SLEEP_TIME_S: float = 0.250


def process_archive(output_dict: Dict[WARCRecordMetadata, WARCJobInformation], warc_path: str):
    for record in stream_records_from_warc(warc_path):
        if record is None:
            continue

        try:
            warc_metadata = extract_metadata_from_warc(record)
        # we are handling the case where the record is an empty string
        except ValueError:
            continue

        output_dict[warc_metadata] = {
            "mappings": [],
            "is_done": False
        }

        text = extract_text_from_html(record)
        tagged_tokens = tokenize_and_tag_raw_text(text)

        # TODO: do something with our tagged tokens!
        # ....
        # TODO: add


def flush_daemon(output_dict: Dict[WARCRecordMetadata, WARCJobInformation], output_path: str):
    while True:
        if len(output_dict) < FLUSH_THRESHOLD:
            time.sleep(DAEMON_SLEEP_TIME_S)
            continue

        with open(output_path, 'w+') as f:
            for warc in output_dict:
                if not output_dict[warc]['is_done']:
                    continue
                for mapping in output_dict[warc]['mappings']:
                    f.write(
                        f'{warc.trec_id}\t{mapping.named_entity}\t{mapping.entity_url}\n')


def init():
    """
    Initializes the program and the utilities.
    """
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s]: %(message)s',
        level=logging.INFO)
    logging.info('initializing NLTK dependencies')
    init_parsing()


def main():
    archives, output_path = parse_cl_args()
    logging.info('parsed list of archives')

    init()
    logging.info('initialization completed')

    # multiprocessing setup
    manager = mp.Manager()
    shared_dict = manager.dict()
    processes: list[mp.Process] = []

    # start daemon which flushes linked entities to file periodically
    mp.Process(target=flush_daemon, args=(shared_dict, output_path)).start()

    for warc in archives:
        logging.info(f'starting job for archive \'{warc}\'')
        process = mp.Process(target=process_archive, args=(shared_dict, warc))
        process.start()
        processes.append(process)

    for process in processes:
        process.join()


if __name__ == '__main__':
    main()
