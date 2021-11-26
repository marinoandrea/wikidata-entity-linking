"""
This scripts generates a TSV dump for specific categories of entities
that have many occurrences in web pages.

The dump is used by the main program to perform a first pass on very 
popular entities.

Arguments
---------
sys.argv[1] - category/label
sys.argv[2] - min. number of attributes to be consider popular (optional, default = 25)
"""

import json
import os
import sys

import elasticsearch as es
import trident


def get_super_entity(label: str) -> str:
    return {
        'person': 'Q5',
        'org': 'Q4830453',
        'software': 'Q218616',
        'city': 'Q515',
        'country': 'Q6256',
        'website': 'Q35127'
    }[label]


def main():
    category = sys.argv[1]

    min_attributes = 25
    if len(sys.argv) > 2:
        min_attributes = int(sys.argv[2])

    if category == 'country':
        with open('scripts/data/countries.json', 'r') as f:
            countries = json.load(f)

    trident_db = trident.Db(os.getenv(
        'KB_PATH', "assets/wikidata-20200203-truthy-uri-tridentdb"))

    es_client = es.Elasticsearch()

    p31 = trident_db.lookup_id('<http://www.wikidata.org/prop/direct/P31>')

    super_entity = trident_db.lookup_id(
        f'<http://www.wikidata.org/entity/{get_super_entity(category)}>')

    cnt_tot = 0
    cnt_pop = 0
    itr = trident_db.s_itr(p31, super_entity)
    entity = itr.field1()
    dump_str = ""
    with open(f"wd-dump_{category}.tsv", 'w') as f:
        while True:
            cnt_tot += 1

            next_entity = itr.field1()

            if len(trident_db.po(entity)) > min_attributes:
                cnt_pop += 1

                wd_uri = trident_db.lookup_str(entity)

                try:
                    es_res = es_client.get(index='wikidata_en', id=wd_uri)

                    label: str = ""
                    for field in es_res['_source']:
                        if field == 'schema_description':
                            continue
                        label = es_res['_source'][field]\
                            .encode()\
                            .decode('unicode_escape')\
                            .encode()\
                            .decode('utf-8')\
                            .strip()

                        if category == 'country':
                            try:
                                country = next(
                                    c for c in countries if c['name'] == label)
                                dump_str += (
                                    f"{entity}\t{country['code']}\t{wd_uri}\n")
                            except StopIteration:
                                pass

                        dump_str += (f"{entity}\t{label}\t{wd_uri}\n")

                except es.NotFoundError:
                    pass

            if cnt_tot % 10_000 == 0:
                os.system('clear')
                print(f'Total: {cnt_tot}')
                print(f'Selected: {cnt_pop}')

            if cnt_pop % 10_000 == 0:
                f.write(dump_str)
                dump_str = ""

            if next_entity == entity or next_entity == -1 or next_entity is None:
                break

            entity = next_entity

        f.write(dump_str)


if __name__ == '__main__':
    main()
