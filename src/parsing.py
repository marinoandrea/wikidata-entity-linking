import typing

import bs4
import spacy

from src.globals import dump_popular_entities
from src.interfaces import EntityLabel, EntityMapping, NamedEntity

NON_RELEVANT_HTML_TAGS = ["script", "style", "link", "noscript"]

spacy_nlp = spacy.load("en_core_web_sm")


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


def extract_entities(text: str) -> typing.Tuple[typing.Set[NamedEntity], typing.List[EntityMapping]]:
    """
    Returns the named entities found in the text and some entity mappings that were found
    using cached results.

    Parameters
    ----------
    text: `str`
    Raw text

    Returns
    -------
    `Tuple[Set[NamedEntity], List[EntityMapping], List[numpy.ndarray]]`
    A tuple containing the Set of labeled named entities that were found [0] and some entity
    mappings which were produced directly from cached values [1].
    """

    doc = spacy_nlp(text)

    # we first perform a simple pass on single tokens and proper nouns
    # and we match them to the popular entities that we have preloaded
    # from trident and elasticsearch. This already produces good mappings
    # with very little computation time.

    cached_mappings: typing.List[EntityMapping] = []
    cached_entities: typing.Set[str] = set()

    def add_preloaded_entity(current_entity: str):
        # we already added this
        if current_entity in cached_entities:
            return
        cached_entity = dump_popular_entities[current_entity]
        cached_entities.add(current_entity)
        cached_mappings.append(
            EntityMapping(named_entity=current_entity,
                          entity_url=cached_entity))

    current_propn: typing.List[str] = []
    for token in doc:

        if token.pos_ == 'PROPN' and len(current_propn) > 0:
            current_propn.append(token.text)

        if not (len(token.text) > 0 and token.text[0].isupper()) or (len(token.text) > 1 and token.text[1].isupper()):
            continue

        # if we find a match with the single token
        if token.text in dump_popular_entities:
            add_preloaded_entity(token.text)
            continue

        token_text_low = token.text.lower()
        if token_text_low in dump_popular_entities:
            add_preloaded_entity(token_text_low)
            continue

        # end of the proper noun group
        if token.pos_ != 'PROPN' and len(current_propn) > 0:
            current_entity = ' '.join(current_propn)
            current_entity_low = ' '.join(current_propn)
            # do we have a match in preloaded entities?
            # if so let's store the mapping without further analysis
            if current_entity in dump_popular_entities:
                add_preloaded_entity(current_entity)
            elif current_entity[:-1] in dump_popular_entities:
                add_preloaded_entity(current_entity[:-1])
            elif current_entity_low in dump_popular_entities:
                add_preloaded_entity(current_entity_low)
            elif current_entity_low[:-1] in dump_popular_entities:
                add_preloaded_entity(current_entity_low[:-1])
            current_propn = []

    # entities that were not matched previously are now added
    # to the pipeline and further processed in the next steps

    entities: typing.Set[NamedEntity] = set()

    for entity in doc.ents:
        if entity.text in cached_entities:
            continue

        # we perform another round of preloaded mappings
        # on entities that were only found by SpaCy to
        # further reduce the amount of entities that will
        # be processed in the computation-heavy pipeline

        if entity.text in dump_popular_entities:
            add_preloaded_entity(entity.text)
            continue
        elif entity.text[:-1] in dump_popular_entities:
            add_preloaded_entity(entity.text[:-1])
            continue

        entity_text_low = entity.text.lower()
        if entity_text_low in dump_popular_entities:
            add_preloaded_entity(entity_text_low)
            continue
        elif entity_text_low[:-1] in dump_popular_entities:
            add_preloaded_entity(entity_text_low[:-1])
            continue

        if not (len(entity.text) > 0 and entity.text[0].isupper()) or (len(entity.text) > 1 and entity.text[1].isupper()):
            continue

        # NOTE(andrea): we may not want to do this but looking at the sample
        # simple numbers and date/times are not considered
        if entity.label_ in {'CARDINAL', 'ORDINAL', 'PERCENT', 'QUANTITY', 'TIME', 'MONEY', 'DATE'}:
            continue

        # we prevent entities from having multiple spaces inside the string
        multiple_spaces_split = entity.text.strip().split('  ')

        for sub_entt in multiple_spaces_split:
            # we don't want URLs
            if sub_entt.startswith('http://') or sub_entt.startswith('https://'):
                continue

            entities.add(NamedEntity(name=sub_entt.strip(),
                                     label=EntityLabel(entity.label_)))

    return entities, cached_mappings
