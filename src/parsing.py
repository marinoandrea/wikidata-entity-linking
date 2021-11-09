import typing

import bs4

NON_RELEVANT_HTML_TAGS = ["script", "style", "link", "noscript"]


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
    text = typing.cast(str, soup.body.get_text())

    # cleaning spaces and newlines
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    text = "\n".join(chunk for chunk in chunks if chunk)

    return text
