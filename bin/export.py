#!/usr/bin/env python

import sys
import os
import os.path

import StringIO
import json
import csv
import pysolr
import logging

import utils

def get_placetypes(opts) :

    solr = pysolr.Solr(opts.solr)

    args = {
        'q': '*:*',
        'fq': '-woeid_superseded_by:*',
        'facet': 'on',
        'facet.field': 'placetype',
        'rows': 0,
        'facet.limit': -1
        }

    rsp = solr.search(**args)
    facets = rsp.facets['facet_fields']['placetype']

    count = len(facets)
    placetypes = {}

    for i in range(0, count, 2):
        placetypes[facets[i]] = facets[i+1]

    return placetypes

def export_place(opts, place, count):

    logging.info("export %s (%s records)" % (place, count))

    dump = "%s.txt" % place
    path_dump = os.path.join(opts.outdir, dump)

    fh = open(path_dump, 'w')

    missing = "%s-nogeo.csv" % place
    path_missing = os.path.join(opts.outdir, missing)

    writer = csv.writer(open(path_missing, 'w'))
    writer.writerow(('woeid', 'name', 'iso'))


    point_features = {}
    poly_features = {}

    solr = pysolr.Solr(opts.solr)
    query = 'placetype:%s' % place

    start = 0
    rows = 10000

    while start <= count:

        args = {
            'q': query,
            'fq': '-woeid_superseded_by:*',
            'rows': rows,
            'start': start
            }

        rsp = solr.search(**args)

        for doc in rsp.docs:

            io = StringIO.StringIO()
            utils.write_json(doc, io)

            io.seek(0)
            fh.write(io.read() + "\n")

            woeid = doc['woeid']
            parent = doc.get('woeid_parent', -1)

            name = doc['name'].encode('utf8')
            iso = doc.get('iso', 'ZZ')	# mainly aerotrpolii - needs to be fixed (20130317/straup)

            centroid = doc.get('centroid', None)

            if not centroid:
                writer.writerow((woeid, name, iso))
                continue

            props = {
                'name': name,
                'woeid': woeid,
                'parent': parent,
                }

            lat,lon = map(float, centroid.split(','))

            point = {
                'type': 'Feature',
                'properties': props,
                'geometry': { 'type': 'Point', 'coordinates': [ lon, lat ] },
                'id': woeid
                }

            if point_features.get(iso, False):
                point_features[iso].append(point)
            else:
                point_features[iso] = [ point ]

            geometry = None

            if doc.get('geometry_default', False):
                geometry = json.loads(doc['geometry_default'])

            elif doc.get('sw_corner', False):

                swlat,swlon = map(float, doc['sw_corner'].split(','))  
                nelat,nelon = map(float, doc['ne_corner'].split(','))  

                coords = [[
                        [swlon, swlat],
                        [swlon, nelat],
                        [nelon, nelat],
                        [nelon, swlat],
                        [swlon, swlat],
                        ]]

                geometry = {
                    'type': 'Polygon',
                    'coordinates': coords
                    }

            else:
                pass

            if not geometry:
                continue

            poly = {
                'type': 'Feature',
                'properties': props,
                'geometry': geometry,
                'id': woeid
                }
            
            if poly_features.get(iso, False):
                poly_features[iso].append(poly)
            else:
                poly_features[iso] = [ poly ]
          
        start += rows
    
    for iso, features in point_features.items():

        geojson = {
            'type': 'FeatureCollection',
            'features': features
            }

        fname = "%s-%s.json" % (place, iso)
        path = os.path.join(opts.outdir, fname)

        fh = open(path, 'w')

        logging.info("write %s" % fname)
        utils.write_json(geojson, fh)

    for iso, features in poly_features.items():

        geojson = {
            'type': 'FeatureCollection',
            'features': features
            }

        fname = "%s-%s-poly.json" % (place, iso)
        path = os.path.join(opts.outdir, fname)

        fh = open(path, 'w')

        logging.info("write %s" % fname)
        utils.write_json(geojson, fh)

if __name__ == '__main__':

    import optparse

    parser = optparse.OptionParser("""export.py --options""")

    parser.add_option("-o", "--outdir", dest="outdir", help="...", default=os.getcwd())
    parser.add_option("-s", "--solr", dest="solr", help="your solr endpoint; default is http://localhost:8983/solr/woedb", default='http://localhost:8983/solr/woedb')
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="enable chatty logging; default is false", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    placetypes = get_placetypes(opts)

    for place, count in placetypes.items():
        export_place(opts, place, count)
