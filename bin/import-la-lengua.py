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

    doc['provider_geometry'] = 'Burrito Justice 2013'

    return doc

def import_gowanus_heights(ll):

    solr = pysolr.Solr(opts.solr)
    docs = []

    geojson = os.path.join(ll, 'la-lengua.json')
    fh = open(geojson, 'r')
    data = json.load(fh)

    f = data['features'][0]
    props = f['properties']

    woeid = props['woe:id']

    docs = []

    doc = {
        'woeid': woeid,
        'woeid_adjacent': props['woe:adjacent'],
        'iso': props['iso'],
        'lang': props['woe:lang'],
        'name': props['name'],
        'placetype': props['woe:placetype'],
        'woeid_parent': props['woe:parent'],
        'provider_metadata': 'Mission Burrito 2013',
        }

    doc = add_geometries(doc, f)
    docs.append(doc)
        
    for adj_woeid in props['woe:adjacent']:

        query = "woeid:%s" % adj_woeid
        rsp = solr.search(q=query)

        if rsp.hits == 0:
            continue

        loc = rsp.docs[0]

        adj = loc.get('woeid_adjacent', [])

        if not woeid in adj:
            adj.append(woeid)
            loc['woeid_adjacent'] = adj
            docs.append(loc)

    print docs
    sys.exit()

    solr.add(docs)
    solr.optimize()

    logging.info("done")

if __name__ == '__main__':

    import optparse

    # As in: http://gowanusheights.info/la-lengua/la-lengua.tar.gz

    parser = optparse.OptionParser("""import-la-lengua.py --options /path/to/la-lengua""")
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

    gh = args[0]

    import_gowanus_heights(gh)
    sys.exit()
