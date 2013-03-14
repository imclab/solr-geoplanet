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
    reader = csv.reader(fh)

    for row in reader:

        woeid = int(row[0])
        geonamesid = int(row[1])

        query = "woeid:%s" % woeid
        rsp = solr.search(q=query)

        if rsp.hits == 0:
            continue

        loc = rsp.docs[0]
        loc['concordance_geonames'] = geonamesid

        docs.append(loc)

        if len(docs) == 100000:
            logging.info("add to solr...")
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

    parser = optparse.OptionParser("""build.py --options geoplanet_data_X.Y.Z.zip""")
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
