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

EPMC_BASE_URL="https://www.ebi.ac.uk/europepmc/webservices/rest/search?resultType=core&pageSize=1000&format=json&"
SEARCH_TEXT = ['HDRUK', 'HDR UK', 'HDR-UK', 'Health Data Research UK']
ACK_FUND_QUERY = " OR ".join(["ACK_FUND:\"{}\"".format(t) for t in SEARCH_TEXT])
AFF_QUERY = " OR ".join(["AFF:\"{}\"".format(t) for t in SEARCH_TEXT])

def request_url(URL):
  """HTTP GET request and load into json"""
  r = requests.get(URL)
  if r.status_code == requests.codes.ok:
    return json.loads(r.text)
  else:
    r.raise_for_status()

def retrieve_papers(query="", data=[], cursorMark="*"):
  DATA=data
  query = urllib.parse.quote_plus(query)
  URL = EPMC_BASE_URL + "&".join(["query=%s" % query, "cursorMark=%s" % cursorMark])
  d = request_url(URL)
  numResults = d['hitCount']
  DATA.extend(d['resultList']['result'])
  if numResults > 1000:
    retrieve_papers(query, DATA, cursorMark=d['nextCursorMark'])
  return DATA

def export_csv(outputFilename, data):
  column_names = ['id', 'doi', 'title', 'authorString', 'authorAffiliations', 'journalTitle', 'pubYear', 'abstract']
  with open(outputFilename, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=column_names)
    writer.writeheader()
    for d in data:
      # Extracting Author affiliations
      authorAffiliations = []
      for author in d['authorList']['author']:
        if 'affiliation' in author.keys():
          authorAffiliations.append(author['affiliation'])
      row = {
        'id': d['id'],
        'doi': "https://doi.org/" + d['doi'],
        'title': d['title'],
        'authorString': d['authorString'],
        'authorAffiliations': " ; ".join(authorAffiliations),
        'journalTitle': d['journalInfo']['journal']['title'],
        'pubYear': d['pubYear'],
        'abstract': d.get('abstractText', '')
      }
      writer.writerow(row)

def main():
  # retrieve papers with funding acknowledgement to HDR-UK
  data = retrieve_papers(query=ACK_FUND_QUERY)
  export_csv('data/papers.csv', data)

  # retrieve papers with author affiliation to HDR-UK
  data = retrieve_papers(query=AFF_QUERY)
  export_csv('data/affiliation.csv', data)

if __name__ == "__main__":
    main()
