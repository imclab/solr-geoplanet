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

        woeid = row['airport_woe_id']

        query = "woeid:%s" % woeid
        rsp = solr.search(q=query)

        if rsp.hits == 0:
            continue

        loc = rsp.docs[0]

        iata_code = row['airport_iata_code']
        icao_code = row['airport_icao_code']

        print "%s: %s" % (woeid, iata_code)

        if iata_code != '':
            loc['concordance_iata'] = iata_code

        if icao_code != '':
            loc['concordance_icao'] = icao_code

        lat = row['airport_woe_latitude']
        lon = row['airport_woe_longitude']

        if lat != '' and lon != '':
            loc['centroid'] = '%s,%s' % (lat, lon)

        """
        swlat = row['airport_sw_latitude']
        swlon = row['airport_sw_longitude']

        if swlat != '' and swlon != '':
            doc['sw_corner'] = '%s,%s' % (swlat, swlon)
        """

        nelat = row['airport_ne_latitude']
        nelon = row['airport_ne_longitude']

        if nelat != '' and nelon != '':
            loc['ne_corner'] = '%s,%s' % (nelat, nelon)

        docs.append(loc)

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
