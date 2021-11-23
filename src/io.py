import time
from typing import List

from src.globals import shared_dict
from src.interfaces import WARCRecordMetadata

DAEMON_SLEEP_TIME_S: float = 5


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
