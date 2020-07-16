#!/usr/bin/env python
# usage: paper-extractor.py
__author__ = "Susheel Varma"
__copyright__ = "Copyright (c) 2019-2020 Susheel Varma All Rights Reserved."
__email__ = "susheel.varma@hdruk.ac.uk"
__license__ = "MIT"

import csv
import json
import urllib
import requests
from pprint import pprint

EPMC_BASE_URL = "https://www.ebi.ac.uk/europepmc/webservices/rest/search?resultType=core&pageSize=1000&format=json&"
SEARCH_TEXT = ['HDRUK', 'HDR UK', 'HDR-UK', 'Health Data Research UK']
ACK_FUND_QUERY = " OR ".join(["ACK_FUND:\"{}\"".format(t) for t in SEARCH_TEXT])
AFF_QUERY = " OR ".join(["AFF:\"{}\"".format(t) for t in SEARCH_TEXT])

COVID_QUERY = "(\"2019-nCoV\" OR \"2019nCoV\" OR \"COVID-19\" OR \"SARS-CoV-2\" OR \"COVID19\" OR \"COVID\" OR \"SARS-nCoV\" OR (\"wuhan\" AND \"coronavirus\") OR \"Coronavirus\" OR \"Corona virus\" OR \"corona-virus\" OR \"corona viruses\" OR \"coronaviruses\" OR \"SARS-CoV\" OR \"Orthocoronavirinae\" OR \"MERS-CoV\" OR \"Severe Acute Respiratory Syndrome\" OR \"Middle East Respiratory Syndrome\" OR (\"SARS\" AND \"virus\") OR \"soluble ACE2\" OR (\"ACE2\" AND \"virus\") OR (\"ARDS\" AND \"virus\") or (\"angiotensin-converting enzyme 2\" AND \"virus\")) AND ((ACK_FUND:\"HDRUK\" OR ACK_FUND:\"HDR UK\" OR ACK_FUND:\"HDR-UK\" OR ACK_FUND:\"Health Data Research UK\") OR (AFF:\"HDRUK\" OR AFF:\"HDR UK\" OR AFF:\"HDR-UK\" OR AFF:\"Health Data Research UK\"))"

# HDR UK Custom tags
NATIONAL_PRIORITIES_CSV = "data/national-priorities.csv"
LAY_SUMMARIES_CSV = "data/lay-summaries.csv"

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

def export_json(data, filename, indent=2):
  with open(filename, 'w') as jsonfile:
    json.dump(data, jsonfile, indent=indent)

def export_csv(data, header, outputFilename):
  with open(outputFilename, 'w') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=header)
    writer.writeheader()
    writer.writerows(data)

def read_csv(filename):
  header = []
  data = []
  with open(filename, newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    header = reader.fieldnames
    for row in reader:
      data.append(row)
  return data, header

NATIONAL_PRIORITIES, NP_HEADER = read_csv(NATIONAL_PRIORITIES_CSV)

def get_national_priorities(d):
  for np in NATIONAL_PRIORITIES:
    if d.get('title') == np['title']:
      return {
        'national priority': np['national priority'],
        'health category': np['health category']
      }
  return {
        'national priority': "",
        'health category': ""
      }

def format_data(data):
  HEADER = ['id', 'doi', 'title', 'authorString', 'authorAffiliations', 'journalTitle', 'pubYear', 'isOpenAccess', 'keywords', 'nationalPriorities', 'healthCategories', 'abstract']
  DATA = []
  for d in data:
    # Get National Priorities & Health Categories
    np = get_national_priorities(d)
    # Extracting Author affiliations
    authorAffiliations = []
    if 'authorList' in d.keys():
      for author in d['authorList']['author']:
        if 'authorAffiliationsList' in author.keys():
          affiliation = "; ".join(author['authorAffiliationsList']['authorAffiliation'])
          authorAffiliations.append(affiliation)
    # Extracting Keywords
    keywords = ""
    if 'keywordList' in d.keys():
      keywords = keywords + "; ".join(d['keywordList']['keyword'])
    row = {
      'id': d.get('id', ''),
      'doi': "https://doi.org/" + d.get('doi',''),
      'title': d.get('title'),
      'authorString': d.get('authorString'),
      'authorAffiliations': "; ".join(authorAffiliations),
      'journalTitle': d.get('journalInfo')['journal']['title'],
      'pubYear': d.get('pubYear'),
      'isOpenAccess': d.get('isOpenAccess'),
      'keywords': keywords,
      'nationalPriorities': np['national priority'],
      'healthCategories': np['health category'],
      'abstract': d.get('abstractText', '')
    }
    DATA.append(row)
  return DATA, HEADER

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
  data, header = format_data(ack_data)
  export_csv(data, header, 'data/acknowledgements.csv')

  # retrieve papers with author affiliation to HDR-UK
  aff_data = retrieve_papers(query=AFF_QUERY, data=[])
  data, header = format_data(aff_data)
  export_csv(data, header, 'data/affiliations.csv')
  
  # retrieve COVID-19 papers with author affiliation or funding acknowledgement to HDR-UK
  covid_data = retrieve_papers(query=COVID_QUERY, data=[])
  data, header = format_data(covid_data)
  export_csv(data, header, 'data/covid-papers.csv')

  # export papers with author affiliation OR funding acknowledgement to HDR-UK
  mergedData = merge('id', ack_data, aff_data)
  data, header = format_data(mergedData)
  export_csv(data, header, 'data/papers.csv')
  export_json(data, 'data/papers.json')


if __name__ == "__main__":
    main()
