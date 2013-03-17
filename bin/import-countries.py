#!/usr/bin/env python

import sys
import os.path

import utils
import csv
import json
import pysolr
import logging
import shapely.geometry

def add_geometries(doc, feature):

    geom = feature['geometry']
    geom = shapely.geometry.asShape(geom)

    centroid = geom.centroid

    lat = centroid.y
    lon = centroid.x

    doc['centroid'] = "%s,%s" % (lat,lon)

    bbox = feature.get('bbox', None)

    if not bbox:
        bbox = geom.bounds

    doc['sw_corner'] = "%s,%s" % (bbox[1], bbox[0])
    doc['ne_corner'] = "%s,%s" % (bbox[3], bbox[2])

    if doc.get('geometry', False):
        del(doc['geometry'])

    doc['geometry_default'] = utils.geometry2carbonite(feature['geometry'])

    props = feature['properties']

    doc['provider_geometry'] = 'natural earth 2.0'

    return doc

def import_countries(countries):

    solr = pysolr.Solr(opts.solr)
    docs = []

    datadir = os.path.join(countries, 'data')

    for root, dirs, files in os.walk(datadir):

        for f in files:

            path = os.path.join(root, f)
            logging.info("processing %s" % path)

            fh = open(path)
            data = json.load(fh)

            f = data['features'][0]
            woeid = f['id']
        
            query = "woeid:%s" % woeid
            rsp = solr.search(q=query)

            if rsp.hits == 0:
                continue

            doc = rsp.docs[0]
            doc = add_geometries(doc, f)

            docs.append(doc)

    solr.add(docs)
    solr.optimize()

    logging.info("done")

if __name__ == '__main__':

    import optparse

    # As in: https://github.com/straup/whereonearth-country

    parser = optparse.OptionParser("""import-countries.py --options /path/to/whereonearth-timezone""")
    parser.add_option("-s", "--solr", dest="solr", help="your solr endpoint; default is http://localhost:8983/solr/woedb", default='http://localhost:8983/solr/woedb')
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="enable chatty logging; default is false", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if len(args) == 0:
        logging.error("You forgot the countries")
        sys.exit()

    countries = args[0]

    import_countries(countries)
    sys.exit()
