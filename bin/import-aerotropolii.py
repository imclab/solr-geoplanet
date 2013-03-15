#!/usr/bin/env python

import sys
import os.path

import csv
import pysolr
import logging

def import_concordance(opts, concord):

    solr = pysolr.Solr(opts.solr)
    docs = []

    fh = open(concord, 'r')
    reader = csv.DictReader(fh)

    for row in reader:

        doc = {
            'woeid': row['aerotropolis_woe_id'],
            'provider': row['derived_from'],
            'name': row['aerotropolis_name'],
            'alias_ENG_V': row['airports'],
            'placetype': 'Areotropolis'
            }

        # TO DO: coords
        # TO DO: hierarchy

        docs.append(doc)

        if len(docs) == 1000:
            logging.info("finish adding to solr...")
            solr.add(docs)
            docs = []

    if len(docs):
        logging.info("finish adding to solr...")
        solr.add(docs)
        docs = []

    solr.optimize()
    logging.info("done")

if __name__ == '__main__':

    import optparse

    parser = optparse.OptionParser("""import-airports.py --options whereonearth-aerotropolis/reference/airports.csv""")
    parser.add_option("-s", "--solr", dest="solr", help="your solr endpoint; default is http://localhost:8983/solr/woedb", default='http://localhost:8983/solr/woedb')
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="enable chatty logging; default is false", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if len(args) == 0:
        logging.error("You forgot the concordances!")
        sys.exit()

    concord = args[0]

    if not os.path.exists(concord):
        logging.error("%s is not a file!" % concord)
        sys.exit()

    import_concordance(opts, concord)
    sys.exit()
