#!/usr/bin/env python3

import os
import sys
import json
import dotenv
import rdflib
import airtable
import requests
import diskcache

from collections import Counter

# get the .env file
dotenv.load_dotenv()

# airtable configuration
key = os.environ.get('AIRTABLE_API_KEY')
base_id = 'appjfPJhxo9IHh8ld'

# use a cache to prevent repeated wikidata/snac lookup
cache = diskcache.Cache('cache', eviction_policy="none")

# the mapping of properties to count
mapping = json.load(open('mapping.json'))

def main():
    counter = Counter()
    for e in get_entities():
        if e['wikidata_id']:
            count_wikidata(e['wikidata_id'], counter)
        if e['snac_id']:
            count_snac(e['snac_id'], counter)

def get_entities():
    table = airtable.Airtable(base_id, 'CPF Authorities', key)
    for page in table.get_iter():
        for record in page:
            f = record['fields'].get
            yield {
                "name": f('Name'),
                "type": f('Entity Type'),
                "snac_id": f('SNAC ID'),
                "wikidata_id": f('Wikidata QCode')
            }


def count_wikidata(wikidata_id, counter):
    g = get_wikidata(wikidata_id)
    print('wd', wikidata_id)

def count_snac(snac_id, counter):
    con = get_snac(snac_id)
    print('snac', snac_id)

@cache.memoize()
def get_snac(snac_id):
    url = 'https://api.snaccooperative.org/'
    data =  {"command": "read", "constellationid": snac_id}
    resp = requests.put(url, json=data)
    if resp.status_code == 200:
        return resp.json()
    else:
        return None


@cache.memoize()
def get_wikidata(wikidata_id):
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.jsonld'
    resp = requests.get(url)
    if resp.status_code == 200:
        return rdflib.Graph().parse(data=resp.text, format='json-ld')
    else:
        return None

#def count_wikidata(wikidata_id, obj, counter):
#    for entity_type, entity_map in mapping['wikidata'].items():
#        for prop_name, prop_id in entity_map.items():
#            pass

if __name__ == "__main__":
    main()
