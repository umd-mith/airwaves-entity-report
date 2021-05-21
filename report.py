#!/usr/bin/env python3

import os
import csv
import sys
import json
import dotenv
import airtable
import jmespath
import requests
import diskcache

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

    # kind of a neat little data structure that lets you count a 
    # hierarchy of things without worrying about missing keys, e.g.
    # counter['Person']['date_of_birth']['wikidata'] += 1
    counter = defaultdict(lambda: defaultdict(Counter))
    entity_count = 0

    # count everything
    for e in get_entities():
        entity_count += 1
        entity_type = e['type']

        if e['wikidata_id']:
            count_wikidata(e['wikidata_id'], counter)
        if e['snac_id']:
            count_snac(e['snac_id'], counter)

    out = csv.writer(open('report.csv', 'w'))
    out.writerow([
        'entity_type',
        'property', 
        'wikidata_count',
        'wikidata_percent',
        'snac_count',
        'snac_percent',
    ])
    for entity_type, entity_counts in counter.items():
        for prop, service_counts in entity_counts.items():

            wikidata_count = service_counts.get('wikidata', 0)
            wikidata_type_count = counter[entity_type]['count']['wikidata']
            wikidata_percent = wikidata_count / wikidata_type_count

            snac_count = service_counts.get('snac', 0)

            snac_type_count = counter[entity_type]['count']['snac']
            if snac_type_count > 0:
                snac_percent = snac_count / snac_type_count
            else:
                snac_percent = 0

            out.writerow([
                entity_type,
                prop,
                wikidata_count,
                wikidata_percent,
                snac_count,
                snac_percent
            ])

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
    print('wikidata', wikidata_id)
    entity = get_wikidata(wikidata_id)
    for entity_type, entity_map in mapping.items():
        for prop_name, queries in entity_map.items():
            query = queries.get('wikidata')
            if not query:
                continue

            counter[entity_type]['count']['wikidata'] += 1

            jq = f'entities.{wikidata_id}.claims.{query}'
            result = jmespath.search(jq, entity)
            if type(result) == list and len(result) > 0:
                counter[entity_type][prop_name]['wikidata'] += 1
            elif result != None:
                sys.exit(f'unexpected jmespath response: {result}')

def count_snac(snac_id, counter):
    print('snac', snac_id)
    entity= get_snac(snac_id)
    for entity_type, entity_map in mapping.items():
        for prop_name, queries in entity_map.items():
            query = queries.get('snac')
            if not query:
                continue

            counter[entity_type]['count']['snac'] += 1

            jq = f'constellation.{query}'
            result = jmespath.search(jq, entity)
            if type(result) == list and len(result) > 0:
                counter[entity_type][prop_name]['snac'] += 1
            if type(result) != list and result != None:
                sys.exit(f'unexpected jmespath response: {result}')

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
