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

# use a cache to prevent repeated wikidata/snac api lookups
cache = diskcache.Cache('cache', eviction_policy="none")

def main():
    write_csv('Person', 'map-person.json')
    write_csv('Corporate Body', 'map-corporate-body.json')

def write_csv(entity_type, map_file):
    entity_map = json.load(open(map_file))
    csv_file = map_file.replace('.json', '.csv')
    out = csv.writer(open(csv_file, 'w'))
    out.writerow(get_col_headers(entity_map))

    for e in get_entities():
        if e['type'] == entity_type:
            out.writerow(get_row(e, entity_map))

def get_col_headers(entity_map):
    col_headers = ['name', 'wikidata_id', 'snac_id']
    for prop in entity_map.keys():
        for service in ['wikidata', 'snac']:
            col_headers.append(f'{prop}_{service}')
    return col_headers

def get_row(entity, entity_map):
    row = [entity['name'], entity['wikidata_id'], entity['snac_id']]
    for prop, queries in entity_map.items():
        for service in ['wikidata', 'snac']:
            result = 0
            query = queries.get(service)
            if query:
                if service == 'wikidata' and entity.get('wikidata_id'):
                    obj = get_wikidata(entity['wikidata_id'])
                    query = f'entities.{entity["wikidata_id"]}.{query}'
                    result = count_results(query, obj)
                    print(entity['wikidata_id'], query, result)
                elif service == 'snac' and entity.get('snac_id'):
                    obj = get_snac(entity['snac_id'])
                    query = f'constellation.{query}'
                    result = count_results(query, obj)
                    print(entity['snac_id'], query, result)
            row.append(result)
    return row

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

def count_results(query, obj):
    result = jmespath.search(query, obj)
    if type(result) == list:
        return 1
    elif type(result) == str:
        return 1
    elif result is not None:
        sys.exit(f'unknown jmespath result: {result}')
    else:
        return 0

if __name__ == "__main__":
    main()
