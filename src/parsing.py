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
