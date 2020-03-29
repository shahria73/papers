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

EPMC_BASE_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search?resultType=core&pageSize=1000&format=json&"
SEARCH_TEXT = ['HDRUK', 'HDR UK', 'HDR-UK', 'Health Data Research UK']
ACK_FUND_QUERY = " OR ".join(["ACK_FUND:\"{}\"".format(t) for t in SEARCH_TEXT])
AFF_QUERY = " OR ".join(["AFF:\"{}\"".format(t) for t in SEARCH_TEXT])


def request_url(URL):
  """HTTP GET request and load into json"""
  r = requests.get(URL)
  if r.status_code != requests.codes.ok:
    r.raise_for_status()
  return json.loads(r.text)


def retrieve_papers(query="", data=None, cursorMark="*"):
  if data is None:
    DATA = []
  else:
    DATA = data
  query = urllib.parse.quote_plus(query)
  URL = EPMC_BASE_URL + "&".join(["query=%s" % query, "cursorMark=%s" % cursorMark])
  print("Retrieving papers from", URL)
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
        'id': d.get('id', ''),
        'doi': "https://doi.org/" + d.get('doi',''),
        'title': d.get('title'),
        'authorString': d.get('authorString'),
        'authorAffiliations': "; ".join(authorAffiliations),
        'journalTitle': d.get('journalInfo')['journal']['title'],
        'pubYear': d.get('pubYear'),
        'abstract': d.get('abstractText', '')
      }
      writer.writerow(row)


def merge(key, *lists):
  import itertools
  from collections import defaultdict
  result = defaultdict(dict)
  for dictionary in itertools.chain.from_iterable(lists):
    result[dictionary[str(key)]].update(dictionary)
  return list(result.values())


def main():
  # retrieve papers with funding acknowledgement to HDR-UK
  ack_data = retrieve_papers(query=ACK_FUND_QUERY, data=[])
  export_csv('data/acknowledgements.csv', ack_data)

  # retrieve papers with author affiliation to HDR-UK
  aff_data = retrieve_papers(query=AFF_QUERY, data=[])
  export_csv('data/affiliations.csv', aff_data)

  # export papers with author affiliation OR funding acknowledgement to HDR-UK
  mergedData = merge('id', ack_data, aff_data)
  export_csv('data/papers.csv', mergedData)


if __name__ == "__main__":
    main()
