import typing

import bs4
import spacy

from src.interfaces import EntityLabel, NamedEntity

NON_RELEVANT_HTML_TAGS = ["script", "style", "link", "noscript"]

perform_ner = spacy.load("en_core_web_sm")


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
        if entity.label_ in {'CARDINAL', 'ORDINAL', 'PERCENT', 'QUANTITY', 'TIME', 'MONEY'}:
            continue

        # we prevent entities
        multiple_spaces_split = entity.text.strip().split('  ')
        for sub_entt in multiple_spaces_split:

            # TODO(andrea): implement capital letter check
            # if len(sub_entt) > 1

            out.add(NamedEntity(name=sub_entt.strip(),
                                label=EntityLabel(entity.label_)))

    return out
