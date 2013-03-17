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

    # What really? Something triggers these errors:
    # Invalid latitude: latitudes are range -90 to 90: provided lat: [-267.734515144]
    # Invalid longitude: longitudes are range -180 to 180: provided lon: [-255.248708048]

    if lat >= -90 and lat <= 90 and lon >= -180 and lon <= 180:
        doc['centroid'] = "%s,%s" % (lat,lon)

    bbox = feature.get('bbox', None)

    if not bbox:
        bbox = geom.bounds
        print bbox

    doc['sw_corner'] = "%s,%s" % (bbox[1], bbox[0])
    doc['ne_corner'] = "%s,%s" % (bbox[3], bbox[2])

    # these really need to be truncated to 6 decimal points...
    # doc['geometry'] = json.dumps(feature['geometry'])

    props = feature['properties']

    doc['provider_geometry'] = 'flickr shapefiles 2.0.1'

    try:
        del(doc['_version_'])
    except Exception, e:
        print "%s : %s" % (doc['woeid'], e)

    return doc

def import_places(opts, place):

    logging.info("importing %s" % place)

    solr = pysolr.Solr(opts.solr)
    docs = []

    fh = open(place, 'r')
    data = json.load(fh)

    counter = 0

    for f in data['features']:

        woeid = f['id']

        query = "woeid:%s" % woeid
        rsp = solr.search(q=query)

        if rsp.hits == 0:
            continue

        doc = rsp.docs[0]
        doc = add_geometries(doc, f)

        # continue

        docs.append(doc)

        if len(docs) == 10000:
            counter += 10000
            logging.info("%s @ %s" % (place, counter))

            solr.add(docs)
            docs = []

    if len(docs):
        counter += len(docs)
        logging.info("%s @ %s" % (place, counter))

        solr.add(docs)
        docs = []

    solr.optimize()
    logging.info("done")

if __name__ == '__main__':

    import optparse

    # As in: http://www.flickr.com/services/shapefiles/2.0.1/

    parser = optparse.OptionParser("""import-airports.py --options whereonearth-aerotropolis/reference/airports.csv""")
    parser.add_option("-s", "--solr", dest="solr", help="your solr endpoint; default is http://localhost:8983/solr/woedb", default='http://localhost:8983/solr/woedb')
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="enable chatty logging; default is false", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if len(args) == 0:
        logging.error("You forgot the shape files")
        sys.exit()

    for place in args :
        import_places(opts, place)

    sys.exit()
