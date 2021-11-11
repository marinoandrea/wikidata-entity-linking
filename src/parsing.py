import datetime
import logging
import re
import typing

import bs4
import nltk
from dateutil import parser as date_parser

from src.interfaces import WARCRecordMetadata

NON_RELEVANT_HTML_TAGS = ["script", "style", "link", "noscript"]


def init_parsing():
    nltk.download('punkt')
    nltk.download('averaged_perceptron_tagger')


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

    w_type: typing.Optional[str] = None
    date: typing.Optional[datetime.datetime] = None
    trec_id: typing.Optional[str] = None
    ip_addr: typing.Optional[str] = None
    digest: typing.Optional[str] = None
    uri: typing.Optional[str] = None
    record_id: typing.Optional[str] = None

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


def extract_text_from_html(page: str) -> str:
    """
    Extracts text from HTML page, removing scripts and other non-relevant
    strings.

    Parameters
    ----------
    page: `str`
    The target HTML page. 

    Returns
    -------
    `str` The extracted text
    """
    soup = bs4.BeautifulSoup(page, features="html.parser")
    for script in soup(NON_RELEVANT_HTML_TAGS):
        script.extract()

    # NOTE(andrea): this casting is just to make mypy happy (bs4 has no typing)
    # we just use the body of the page here but we may want to also include the head
    text = typing.cast(str, soup.get_text())

    # cleaning spaces and newlines
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = "\n".join(chunk for chunk in chunks if chunk)

    return text


def tokenize_and_tag_raw_text(text: str) -> list[tuple[str, str]]:
    """
    Returns a list of tuples in the form:
    - [0] token
    - [1] POS tag

    Parameters
    ----------
    text: `str`
    Raw text. 

    Returns
    -------
    `list[tuple[str, str]]` POS tagged tokens
    """
    tokens = nltk.word_tokenize(text)
    return nltk.pos_tag(tokens)  # type: ignore
