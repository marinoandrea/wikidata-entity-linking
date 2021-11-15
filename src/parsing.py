import typing

import bs4
import nltk

from src.interfaces import NamedEntity

NON_RELEVANT_HTML_TAGS = ["script", "style", "link", "noscript"]


def init_parsing():
    nltk.download('punkt')
    nltk.download('averaged_perceptron_tagger')
    nltk.download('maxent_ne_chunker')
    nltk.download('words')


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
    # text = typing.cast(str, soup.get_text())

    # cleaning spaces and newlines
    # TODO(andrea): find out whether this string manipulation (which
    # should be optimized anyway) is actually needed
    # lines = (line.strip() for line in text.splitlines())
    # chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # text = "\n".join(chunk for chunk in chunks if chunk)

    return typing.cast(str, soup.get_text())


def tokenize_and_tag_raw_text(text: str) -> typing.List[typing.Tuple[str, str]]:
    """
    Returns a list of typing.Tuples in the form:
    - [0] token
    - [1] POS tag

    Parameters
    ----------
    text: `str`
    Raw text. 

    Returns
    -------
    `List[typing.Tuple[str, str]]` POS tagged tokens
    """
    tokens = nltk.word_tokenize(text)
    return nltk.pos_tag(tokens)  # type: ignore


def extract_entities(tokens: typing.List[typing.Tuple[str, str]]):
    """
    Returns the named entities found in the provided List of tokens.

    Parameters
    ----------
    tokens: `List[typing.Tuple[str, str]]`
    List of pos tagged extracted tokens. 

    Returns
    -------
    `List[NamedEntity]` List of labeled named entities.
    """
    chunks = nltk.ne_chunk(tokens)
    # NOTE(andrea): we are explicitly not removing duplicates
    # given the format of the output sample.
    named_entities: typing.List[NamedEntity] = []
    for chunk in chunks:
        # here we are removing non-labeled entities and other
        # words from the pipeline
        label = getattr(chunk, 'label', lambda: 'UNKNOWN')()
        if label == 'UNKNOWN':
            continue
        # we obtain the entity original string from the tokens
        name = ' '.join(token[0] for token in chunk)
        named_entities.append(NamedEntity(name=name, label=label))
    return named_entities
