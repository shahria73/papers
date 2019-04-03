#!/usr/bin/env python
# usage: paper-extractor.py
__author__ = "Susheel Varma"
__copyright__ = "Copyright (c) 2019 Susheel Varma All Rights Reserved."
__email__ = "susheel.varma@ebi.ac.uk"
__license__ = "MIT"

import csv
import json
import urllib
import requests

EPMC_BASE_URL="https://www.ebi.ac.uk/europepmc/webservices/rest/search?pageSize=1000&format=json&"
QUERY="ACK_FUND:\"HDRUK\" OR ACK_FUND:\"HDR UK\" OR ACK_FUND:\"HDR-UK\" OR ACK_FUND:\"Health Data Research UK\""

DATA = []

def request_url(URL):
  """HTTP GET request and load into json"""
  r = requests.get(URL)
  if r.status_code == requests.codes.ok:
    return json.loads(r.text)
  else:
    r.raise_for_status()

def retrieve_papers(data=DATA, query=QUERY, cursorMark="*"):
  query = urllib.parse.quote_plus(query)
  URL = EPMC_BASE_URL + "&".join(["query=%s" % query, "cursorMark=%s" % cursorMark])
  d = request_url(URL)
  numResults = d['hitCount']
  DATA.extend(d['resultList']['result'])
  if numResults > 1000:
    retrieve_papers(DATA, cursorMark=d['nextCursorMark'])

def export_csv():
  column_names = ['id', 'doi', 'title', 'authorString', 'journalTitle', 'pubYear']
  with open('papers.csv', 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=column_names)
    writer.writeheader()
    for d in DATA:
      row = {
        'id': d['id'],
        'doi': d['doi'],
        'title': d['title'],
        'authorString': d['authorString'],
        'journalTitle': d['journalTitle'],
        'pubYear': d['pubYear']
      }
      writer.writerow(row)

def main():
  retrieve_papers()
  export_csv()

if __name__ == "__main__":
    main()
