#!/usr/bin/env python

# http://www.doughellmann.com/PyMOTW/zipfile/
# http://docs.python.org/2/library/zipfile

import zipfile
import sys
import unicodecsv

def parse_zipfile(path):

    version = 'fix me'

    zf = zipfile.ZipFile(path)

    for info in zf.infolist():

        fname = info.filename

        if fname.startswith('geoplanet_adjacencies'):
            parse_adjacencies(zf, fname, version)

        elif fname.startswith('geoplanet_aliases'):
            parse_aliases(zf, fname, version)

        elif fname.startswith('geoplanet_places'):
            parse_places(zf, fname, version)
        
        else:
            pass


def parse_places(zf, fname, version):

    return 

    fh = zf.open(fname)
    reader = unicodecsv.UnicodeReader(fh, delimiter='\t')

    for row in reader:
        print row

def parse_adjacencies(zf, fname, version):

    return 

    fh = zf.open(fname)
    reader = unicodecsv.UnicodeReader(fh, delimiter='\t')

    for row in reader:
        print row

def parse_aliases(zf, fname, version):

    fh = zf.open(fname)
    reader = unicodecsv.UnicodeReader(fh, delimiter='\t')

    for row in reader:
        print row

if __name__ == '__main__':

    path = sys.argv[1]

    parse_zipfile(path)

