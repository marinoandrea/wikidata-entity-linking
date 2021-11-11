import datetime
import gzip
import logging
import re
from typing import Generator, Optional

from dateutil import parser as date_parser

from src.interfaces import WARCRecordMetadata

WARC_VERSION = 1.0


def stream_records_from_warc(path: str) -> Generator[str, None, None]:
    """
    This functions streams WARC records from WARC files.

    Note: It only supports WARC v1.0.

    Parameters
    ----------
    `path` A valid WARC file path.

    Returns
    -------
    `Generator[str, None, None]` A generator of WARC record strings. 
    """
    with gzip.open(path, 'rt', errors='ignore') as fd:
        payload: str = ''
        for line in fd:
            if line.strip() == f"WARC/{WARC_VERSION}":
                yield payload
                payload = ''
            else:
                payload += line
        yield payload


def extract_metadata_from_warc(warc_record: str) -> WARCRecordMetadata:
    """
    Extracts metadata from WARC record.

    Parameters
    ----------
    warc_record: `str`
    The target WARC record string. 

    Returns
    -------
    `WARCRecordMetadata` The extracted WARC metadata dataclass.
    """

    w_type: Optional[str] = None
    date: Optional[datetime.datetime] = None
    trec_id: Optional[str] = None
    ip_addr: Optional[str] = None
    digest: Optional[str] = None
    uri: Optional[str] = None
    record_id: Optional[str] = None

    for line in warc_record.splitlines():
        w_type_match = re.search(r'^WARC-Type: ([a-zA-Z0-9]+)$', line)
        if w_type_match is not None:
            w_type = w_type_match.groups()[0]

        try:
            date_match = re.search(r'^WARC-Date: ([a-zA-Z0-9\:\-]+)$', line)
            if date_match is not None:
                date = date_parser.parse(date_match.groups()[0])
        except date_parser.ParserError:
            logging.log(
                logging.WARN, f"warc record '{trec_id}' date format is not correct")
            continue

        trec_id_match = re.search(
            r'^WARC-(Trec|TREC)-ID: ([a-zA-Z0-9\-]+)$', line)
        if trec_id_match is not None:
            trec_id = trec_id_match.groups()[1]

        ip_addr_match = re.search(
            r'^WARC-IP-Address: ([a-zA-Z0-9\-\.]+)$', line)
        if ip_addr_match is not None:
            ip_addr = ip_addr_match.groups()[0]

        digest_match = re.search(
            r'^WARC-Payload-Digest: ([a-zA-Z0-9\-\.\:]+)$', line)
        if digest_match is not None:
            digest = digest_match.groups()[0]

        uri_match = re.search(r'^WARC-Target-URI: (.+)$', line)
        if uri_match is not None:
            uri = uri_match.groups()[0]

        record_id_match = re.search(r'^WARC-Record-ID: (.+)$', line)
        if record_id_match is not None:
            record_id = record_id_match.groups()[0]
            # NOTE(andrea): here we are assuming that record id is the last field
            # if this is not always trough then we must find a different way to deal
            # with WARC metadata parsing without scanning the whole file everytime.
            break

    if trec_id is None:
        logging.log(
            logging.ERROR, f"warc record does not have a trec ID")
        raise ValueError()

    return WARCRecordMetadata(
        trec_id=trec_id,
        w_type=w_type,
        date=date,
        ip_addr=ip_addr,
        digest=digest,
        uri=uri,
        record_id=record_id
    )
