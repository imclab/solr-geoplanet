#!/usr/bin/env python

import sys
import os.path

import utils
import csv
import json
import pysolr
import logging
import shapely.geometry

def add_geometries(opts, doc):

    datadir = os.path.join(aerotropolii, 'data', 'geojson')
    tree = utils.woeid2path(doc['woeid'])
    fname = "%s.json" % doc['woeid']

    path = os.path.join(datadir, tree, fname)

    fh = open(path)
    data = json.load(fh)

    feature = data['features'][0]

    geom = feature['geometry']
    geom = shapely.geometry.asShape(geom)

    centroid = geom.centroid
    lat = centroid.y
    lon = centroid.x

    bbox = feature['bbox']

    doc['centroid'] = "%s,%s" % (lat,lon)

    doc['sw_corner'] = "%s,%s" % (bbox[1], bbox[0])
    doc['ne_corner'] = "%s,%s" % (bbox[3], bbox[2])

    doc['geometry_default'] = utils.geometry2carbonite(feature['geometry'])

    props = feature['properties']

    area = props.get('area_sqkm', None)
    rank = props.get('scalerank', None)

    if area:
        doc['area'] = float(area)

    if rank:
        doc['scale_rank'] = int(rank)

    doc['provider_geometry'] = 'natural earth 2.0'
    return doc

def import_airports(opts,  aerotropolii):

    solr = pysolr.Solr(opts.solr)
    docs = []

    concord = os.path.join(aerotropolii, 'reference', 'airports.csv')

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

        bbox = row['airport_woe_bbox']
        bbox = bbox.split(',')
        bbox = map(float, bbox)

        loc['sw_corner'] = '%s,%s' % (bbox[0], bbox[1])
        loc['ne_corner'] = '%s,%s' % (bbox[2], bbox[3])

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

def import_aerotropolii(opts, aerotropolii):

    solr = pysolr.Solr(opts.solr)
    docs = []

    concord = os.path.join(aerotropolii, 'reference', 'aerotropolii.csv')

    fh = open(concord, 'r')
    reader = csv.DictReader(fh)

    for row in reader:

        doc = {
            'woeid': row['aerotropolis_woe_id'],
            'provider_metadata': ['natural earth 2.0', 'woedb 2.0'],
            'name': row['aerotropolis_name'],
            'alias_ENG_V': row['airports'],
            'placetype': 'Aerotropolis'
            }

        doc = add_geometries(opts, doc)

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

    # As in: https://github.com/straup/whereonearth-aerotropolis

    parser = optparse.OptionParser("""import-airports.py --options /path/to/whereonearth-aerotropolis""")
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

    aerotropolii = args[0]
    
    import_aerotropolii(opts, aerotropolii)
    import_airports(opts, aerotropolii)

    sys.exit()
