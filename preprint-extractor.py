#!/usr/bin/env python
# usage: preprint-extractor.py
__author__ = "Susheel Varma"
__copyright__ = "Copyright (c) 2020 Susheel Varma All Rights Reserved."
__email__ = "susheel.varma@ebi.ac.uk"
__license__ = "MIT"

import csv
import json
import urllib
import requests
from operator import itemgetter
from pprint import pprint
from fuzzywuzzy import fuzz
from fuzzywuzzy import process

MEDARXIV_URL = "https://connect.medrxiv.org/relate/collection_json.php?grp=181"
HDRUK_MEMBERS_CSV = "$HOME/secrets/contacts.csv"

def request_url(URL):
    """HTTP GET request and load into json"""
    r = requests.get(URL)
    if r.status_code != requests.codes.ok:
        r.raise_for_status()
    return json.loads(r.text)

def read_csv(filename):
    print("Reading CSV:", filename)
    rows = []
    with open(filename) as csvfile:
        reader = csv.DictReader(csvfile)
        header = reader.fieldnames
        for row in reader:
            rows.append(row)
    return header, rows

def write_csv(filename, header, rows):
    print("Writing CSV to:", filename)
    with open(filename, 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

def download_arxiv_preprints(URL=MEDARXIV_URL):
    print("Downloading data from URL:", URL)
    data = request_url(URL)
    return data

def filter_preprints(preprints, authors):
    data = []
    author_full_names = [a['Full Name'] for a in authors]
    affiliations = [a['Affiliation'] for a in authors]
    for p in preprints:
        doi = p.get('rel_doi', "")
        authors = [a['author_name'] for a in p['rel_authors']]
        affiliations = [a['author_inst'] for a in p['rel_authors']]
        doi_max_author_match = 0
        doi_max_affiliation_match = 0
        print("Processing authors and affiliations for doi:", doi)
        for preprint_author in authors:
            matches = process.extract(preprint_author, author_full_names, scorer=fuzz.token_set_ratio)
            match_value = max(matches, key = itemgetter(1))[1]
            if int(match_value) > int(doi_max_author_match): doi_max_author_match = match_value
        for preprint_affiliation in affiliations:
            matches = process.extract(preprint_affiliation, affiliations, scorer=fuzz.token_set_ratio)
            match_value = max(matches, key = itemgetter(1))[1]
            if int(match_value) > int(doi_max_affiliation_match): doi_max_affiliation_match = match_value
        if doi_max_author_match > 90 and doi_max_affiliation_match > 90:
            row = {
                'site': p.get('rel_site', ""),
                'doi': doi,
                'date': p.get('rel_date',""),
                'link': p.get('rel_link', ""),
                'title': p.get('rel_title', ""),
                'authors': "; ".join(authors),
                'affiliations': "; ".join(affiliations),
                'abstract': p.get('rel_abs', ""),
                'author_similarity': doi_max_author_match,
                'affiliation_similarity': doi_max_affiliation_match
            }

            print(row)
            data.append(row)
    return data


def main():
    headers, authors = read_csv(HDRUK_MEMBERS_CSV)
    preprints = download_arxiv_preprints()
    preprints = preprints["rels"]
    filtered_preprints = filter_preprints(preprints, authors)
    headers = ['site', 'doi', 'date', 'link', 'title', 'authors', 'affiliations', 'abstract', 'author_similarity', 'affiliation_similarity']
    write_csv("data/covid-preprints.csv", headers, filtered_preprints)

if __name__ == "__main__":
    main()