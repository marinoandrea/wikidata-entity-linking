import typing

import bs4
import en_core_web_sm

from src.interfaces import NamedEntity

NON_RELEVANT_HTML_TAGS = ["script", "style", "link", "noscript"]

perform_ner = en_core_web_sm.load()


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
    html_start_idx = page.find('<!DOCTYPE')

    if html_start_idx == -1:
        return ''

    soup = bs4.BeautifulSoup(page[html_start_idx:], features="html.parser")
    for tag in soup(NON_RELEVANT_HTML_TAGS):
        tag.extract()

    return typing.cast(str, soup.body.get_text().strip().replace("\n", " ").replace("\r", " "))


def extract_entities(text: str) -> typing.Set[NamedEntity]:
    """
    Returns the named entities found in the provided List of tokens.

    Parameters
    ----------
    text: `str`
    Raw text

    Returns
    -------
    `Set[NamedEntity]` Set of labeled named entities.
    """
    out: typing.Set[NamedEntity] = set()
    doc = perform_ner(text)
    for entity in doc.ents:
        # NOTE(andrea): we may not want to do this but looking at the sample
        # simple numbers are not considered
        if entity.label_ in ['CARDINAL', 'ORDINAL']:
            continue
        n_entity = NamedEntity(name=entity.text.strip(), label=entity.label_)
        out.add(n_entity)
    return out
