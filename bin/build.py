#!/usr/bin/env python

# http://www.doughellmann.com/PyMOTW/zipfile/
# http://docs.python.org/2/library/zipfile

import pysolr
import zipfile
import sys
import unicodecsv
import re
import os.path
import logging

def parse_zipfile(path):

    pattern = re.compile("geoplanet_data_([\d\.]+)\.zip$")
    match = pattern.match(os.path.basename(path))

    if not match:
        logging.error("failed to match version number!")
        return False

    groups = match.groups()
    version = groups[0]

    places = 'geoplanet_places_%s.tsv' % version
    aliases = 'geoplanet_aliases_%s.tsv' % version
    adjacencies = 'geoplanet_adjacencies_%s.tsv' % version
    changes = 'geoplanet_changes_%s.tsv' % version
    
    zf = zipfile.ZipFile(path)

    file_list = []

    for i in zf.infolist():
        file_list.append(i.filename)

    if not places in file_list:
        logging.error("Missing %s" % places)
        return False

    parse_places(zf, places, version)

    parse_aliases(zf, aliases, version)

    parse_adjacencies(zf, adjacencies, version)

    if changes in file_list:
        # apply changes...
        pass

def parse_places(zf, fname, version):

    # hack - make me a global or ... ?

    solr = pysolr.Solr('http://localhost:8983/solr/woedb')

    fh = zf.open(fname)
    reader = unicodecsv.UnicodeReader(fh, delimiter='\t')

    docs = []

    for row in reader:

        new = {
            'woeid': row['WOE_ID'],
            'parent_woeid': row['Parent_ID'],
            'name': row['Name'],
            'placetype': row['PlaceType'],
            'lang' : row['Language'],
            'iso': row['ISO'],
            'provider': 'geoplanet %s' % version,
            }

        query = "woeid:%s" % row['WOE_ID']

        rsp = solr.search(q=query)

        if rsp.hits:
            old = rsp.docs[0]
            del(old['date_indexed'])

            new = dict(old.items() + new.items())

        docs.append(new)

        if len(docs) == 10000:
            solr.add(docs)
            docs = []

    if len(docs) == 1000:
        solr.add(docs)

def parse_adjacencies(zf, fname, version):

    return 

    fh = zf.open(fname)
    reader = unicodecsv.UnicodeReader(fh, delimiter='\t')

    for row in reader:
        print row

def parse_aliases(zf, fname, version):

    return

    fh = zf.open(fname)
    reader = unicodecsv.UnicodeReader(fh, delimiter='\t')

    for row in reader:
        print row

if __name__ == '__main__':

    path = sys.argv[1]

    parse_zipfile(path)

