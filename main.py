import logging

from src.cli import parse_cl_args
from src.parsing import (extract_text_from_html, init_parsing,
                         tokenize_and_tag_raw_text)
from src.warc import stream_records_from_warc


def init():
    """
    Initializes the program and the utilities.
    """
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s]: %(message)s',
        level=logging.DEBUG)
    logging.info('initializing NLTK dependencies')
    init_parsing()


def main():
    init()
    logging.info('initialization completed')

    archives = parse_cl_args()
    logging.info('parsed list of archives')

    for warc in archives:
        # TODO: this for-loop will change whenever we introduce multiprocessing
        for record in stream_records_from_warc(warc):
            text = extract_text_from_html(record)
            tagged_tokens = tokenize_and_tag_raw_text(text)

            # TODO: do something with our tagged tokens!
            # ....
        logging.info(f'extracted all records from archive \'{warc}\'')


if __name__ == '__main__':
    main()
