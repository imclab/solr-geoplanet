#!/usr/bin/env python

import sys
import utils
import json
import os
import os.path
import logging

if __name__ == '__main__':

    import optparse

    parser = optparse.OptionParser("""export.py --options""")

    parser.add_option("-e", "--export", dest="export", help="...", default=None)
    parser.add_option("-o", "--outdir", dest="outdir", help="...", default=None)
    parser.add_option("-p", "--placetype", dest="placetype", help="...", default=None)
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="enable chatty logging; default is false", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    placetype = opts.placetype
    fname = "%s-poly.json" % placetype

    path = os.path.join(opts.export, placetype, fname)

    if not os.path.exists(path):
        logging.error("%s is not a file!" % path)
        sys.exit()

    fh = open(path, 'r')
    data = json.load(fh)

    for f in data['features']:

        geojson = {
            'type': 'FeatureCollection',
            'features': [ f ]
        }

        woeid = f['id']

        tree = utils.woeid2path(woeid)
        fname = "%s.json" % woeid

        root = os.path.join(opts.outdir, tree)
        path = os.path.join(root, fname)

        if not os.path.exists(root):
            logging.info("create %s" % root)
            os.makedirs(root)

        logging.info("write %s" % path)

        out = open(path, 'w')
        utils.write_json(geojson, out)

    sys.exit()
