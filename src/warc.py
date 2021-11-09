import gzip
from typing import Generator

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
