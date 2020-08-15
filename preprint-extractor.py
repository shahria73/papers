#!/usr/bin/env python
# usage: preprint-extractor.py
__author__ = "Susheel Varma"
__copyright__ = "Copyright (c) 2019-2020 Susheel Varma All Rights Reserved."
__email__ = "susheel.varma@hdruk.ac.uk"
__license__ = "MIT"

import sys
import time
import csv
import json
import urllib
from datetime import datetime
import requests
from operator import itemgetter
from pprint import pprint
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

# Based on https://api.biorxiv.org/covid19/help
BIORXIV_COVID_API_URL = "https://api.biorxiv.org/covid19/{}"
HDRUK_MEMBERS_CSV = "/home/runner/secrets/contacts.csv"

def request_url(URL):
    """HTTP GET request and load into json"""
    r = requests.get(URL)
    if r.status_code != requests.codes.ok:
        r.raise_for_status()
    return json.loads(r.text)

def read_json(filename):
  with open(filename, 'r') as file:
    return json.load(file)

def write_json(data, filename, indent=2):
  with open(filename, 'w') as jsonfile:
    json.dump(data, jsonfile, indent=indent)

def write_csv(data, header, outputFilename):
  with open(outputFilename, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=header)
    writer.writeheader()
    writer.writerows(data)

def read_csv(filename):
  header = []
  data = []
  with open(filename, mode='r', encoding='utf-8-sig', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    header = reader.fieldnames
    for row in reader:
      data.append(row)
  return data, header

def retrieve_preprints(BASE_URL, data=None, page=0):
    DATA = data or []
    URL = BASE_URL.format(page)
    print("Retrieving preprints from", URL)
    d = request_url(URL)
    cursor = int(d['messages'][0]['cursor'])
    count = int(d['messages'][0]['count'])
    total = int(d['messages'][0]['total'])
    page = (cursor+count)+1
    print("cursor:{} count:{} total:{}".format(cursor, count, total))
    DATA.extend(d['collection'])

    if page < total:
        time.sleep(0.2)
        retrieve_preprints(BIORXIV_COVID_API_URL, DATA, page)
    return DATA

def filter_preprints(preprints):
    found = 0
    data = []
    members, header = read_csv(HDRUK_MEMBERS_CSV)
    # author_full_names = [a['Full Name'] for a in authors]
    # affiliations = [a['Affiliation'] for a in authors]
    authors = []
    affiliations = []
    for a in members:
        authors.append(a['Full Name'])
        affiliations.append(a['Affiliation'])
    affiliations = list(set(affiliations))
    affiliations.extend(["HDRUK", "HDR UK", "HDR-UK", "HEALTH DATA RESEARCH UK", "HEALTH DATA RESEARCH UK LTD"])

    num_preprints = len(preprints)
    for i, p in enumerate(preprints):
        doi = p.get('rel_doi', "")
        preprint_authors = []
        preprint_affiliations = []
        if p.get('rel_authors', None) is not None:
            preprint_authors = [a['author_name'] for a in p['rel_authors']]
            preprint_affiliations = [a['author_inst'] for a in p['rel_authors']]
            doi_max_author_match = 0
            doi_max_affiliation_match = 0
            print("{}/{} ({}) Processing authors and affiliations for doi:{}".format(i+1, num_preprints, found, doi))
            # Fuzzy match author
            for preprint_author in preprint_authors:
                matches = process.extract(preprint_author, authors, scorer=fuzz.token_set_ratio)
                match_value = max(matches, key = itemgetter(1))[1]
                if int(match_value) > int(doi_max_author_match): doi_max_author_match = match_value
            # Fuzzy match affilaition
            for preprint_affiliation in preprint_affiliations:
                matches = process.extract(preprint_affiliation, affiliations, scorer=fuzz.token_set_ratio)
                match_value = max(matches, key = itemgetter(1))[1]
                if int(match_value) > int(doi_max_affiliation_match): doi_max_affiliation_match = match_value
            print("Author Match: {} | Affiliation Match: {}".format(doi_max_author_match, doi_max_affiliation_match))
            if doi_max_author_match > 90 and doi_max_affiliation_match > 90:
                found += 1
                row = {
                    'site': p.get('rel_site', ""),
                    'doi': doi,
                    'date': p.get('rel_date',""),
                    'link': p.get('rel_link', ""),
                    'title': p.get('rel_title', ""),
                    'authors': "; ".join(preprint_authors),
                    'affiliations': "; ".join(preprint_affiliations),
                    'abstract': p.get('rel_abs', ""),
                    'category': p.get('category', ""),
                    'author_similarity': doi_max_author_match,
                    'affiliation_similarity': doi_max_affiliation_match
                }

                print(row)
                data.append(row)
    return data

def generate_summary(preprints):
    data = []
    headers = []
    summaries = {}
    for p in preprints:
        date_obj = datetime.strptime(p['date'], "%Y-%m-%d")
        month_year = datetime.strftime(date_obj, "%b-%y")
        if month_year not in summaries.keys(): summaries[month_year] = {"Total": 0}
        summaries[month_year]['Total'] += 1
        if p['category'] not in summaries[month_year].keys(): summaries[month_year][p['category']] = 0
        summaries[month_year][p['category']] += 1
    for month, summary in summaries.items():
        row = { 'month': month}
        row.update(summary)
        headers.extend(row.keys())
        data.append(row)
    headers = list(set(headers))
    return data, headers


def main():
    # read old preprint extract
    old_data = read_json('data/covid/raw-preprints.json')

    # retrieve new preprint extract
    data = retrieve_preprints(BIORXIV_COVID_API_URL)
    write_json(data, 'data/covid/raw-preprints.json')

    # check if length of new extract is more than old extract. fail if not.
    if len(old_data) >= len(data):
        print("Error: New extract ({}) smaller than previous extract ({})".format(len(data), len(old_data)))
        sys.exit(1)
    # filter preprints for HDR UK authors and affilaitions
    data = filter_preprints(data)
    write_json(data, 'data/covid/preprints.json')
    headers = ['site', 'doi', 'date', 'link', 'title', 'authors', 'affiliations', 'abstract', 'category', 'author_similarity', 'affiliation_similarity']
    write_csv(data, headers, "data/covid/preprints.csv")

    # generate preprint summary
    summary, headers = generate_summary(data)
    write_csv(summary, headers, "data/covid/preprints-summary.csv")
    


if __name__ == "__main__":
    main()