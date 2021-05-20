#!/usr/bin/env python3

import os
import sys
import json
import dotenv
import rdflib
import airtable
import requests
import diskcache

from jmespath import search as q
from collections import defaultdict, Counter

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

    # kind of neat little datastructure that lets you count things 
    # by doing this without worry about missing keys:
    # > counter['wikidata']['person']['birth_data'] += 1
    counter = defaultdict(lambda: defaultdict(Counter))

    # keep track of number of entities
    entity_count = wikidata_count = snac_count = 0

    # count everything
    for e in get_entities():
        entity_count += 1
        if e['wikidata_id']:
            wikidata_count += 1 
            count_wikidata(e['wikidata_id'], counter)
        if e['snac_id']:
            snac_count += 1
            count_snac(e['snac_id'], counter)

        if wikidata_count > 25:
            break

    for service, entity_counts in counter.items():
        for entity_type, prop_counts in entity_counts.items():
            for prop, count in prop_counts.items():
                print(service, entity_type, prop, count, count / entity_count)

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
    e = get_wikidata(wikidata_id)
    for entity_type, entity_map in mapping['wikidata'].items():
        for prop_name, query in entity_map.items():
            result = q(f'entities.{wikidata_id}.{query}', e)
            if type(result) == list and len(result) > 0:
                counter['wikidata'][entity_type][prop_name] += 1
            print(entity_type, prop_name, query)


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
    url = f'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json'
    resp = requests.get(url)
    if resp.status_code == 200:
        return resp.json()
    else:
        return None

if __name__ == "__main__":
    main()
