#!/usr/bin/env python

# http://www.doughellmann.com/PyMOTW/zipfile/
# http://docs.python.org/2/library/zipfile

import pysolr
import zipfile
import sys
import unicodecsv
import re
import os.path
import logging

class woedb:

    def __init__ (self,):

        self.solr = pysolr.Solr('http://localhost:8983/solr/woedb')
        self.version = None
        self.zf = None

    def parse_zipfile(self, path):

        pattern = re.compile("geoplanet_data_([\d\.]+)\.zip$")
        match = pattern.match(os.path.basename(path))

        if not match:
            logging.error("failed to match version number!")
            return False

        groups = match.groups()
        self.version = groups[0]

        places = 'geoplanet_places_%s.tsv' % self.version
        aliases = 'geoplanet_aliases_%s.tsv' % self.version
        adjacencies = 'geoplanet_adjacencies_%s.tsv' % self.version
        changes = 'geoplanet_changes_%s.tsv' % self.version
    
        self.zf = zipfile.ZipFile(path)

        file_list = []

        for i in self.zf.infolist():
            file_list.append(i.filename)

        if not places in file_list:
            logging.error("Missing %s" % places)
            return False

        self.parse_places(places)

        self.parse_aliases(aliases)

        self.parse_adjacencies(adjacencies)

        if changes in file_list:
            # apply changes...
            pass

    def parse_places(self, fname):

        logging.debug("parse places %s" % fname)
        return

        reader = self.zf_reader(fname)
        docs = []

        for row in reader:

            new = {
                'woeid': row['WOE_ID'],
                'parent_woeid': row['Parent_ID'],
                'name': row['Name'],
                'placetype': row['PlaceType'],
                'lang' : row['Language'],
                'iso': row['ISO'],
                }

            new = self.foo(new)

        docs.append(new)

        if len(docs) == 10000:
            solr.add(docs)
            docs = []

        if len(docs) == 1000:
            solr.add(docs)

    def parse_adjacencies(zf, fname, version):

        logging.debug("parse adjacencies %s" % fname)
        return 

    def parse_aliases(self, fname):

        logging.debug("parse aliases %s" % fname)

        reader = self.zf_reader(fname)
        docs = []
        new = {}

        for row in reader:

            woeid = row['WOE_ID']
            prev = new.get('woeid')

            print "%s -> %s" % (woeid, prev)

            alias_k = "alias_%s_%s" % (row['Language'], row['Name_Type'])
            alias_v = row['Name']

            if prev and prev != woeid:

                new = self.foo(new)
                docs.append(new)

                new = {}

                if len(docs) == 10000:
                    print "ADD docs"
                    self.solr.add(docs)
                    docs = []

            #

            if new.get(woeid, False):

                if new.get(alias_k, False):
                    new[ alias_k ].append(alias_v)
                else:
                    new[ alias_k ] = [ alias_v ]

            else:
                new = { 'woeid': woeid }
                new[ alias_k ] = [ alias_v ]

        #

        if len(new.keys()):
            new = self.foo(new)
            docs.append(new)
            new = {}

        if len(docs):
            self.solr.add(docs)

    def zf_reader(self, fname):

        fh = self.zf.open(fname)
        return unicodecsv.UnicodeReader(fh, delimiter='\t')

    def foo (self, new):

        new['provider'] = 'geoplanet %s' % self.version

        query = "woeid:%s" % new['woeid']

        rsp = self.solr.search(q=query)

        if rsp.hits:
            old = rsp.docs[0]
            del(old['date_indexed'])
            del(old['_version_'])

            new = dict(old.items() + new.items())

        return new
        
if __name__ == '__main__':

    path = sys.argv[1]

    w = woedb()
    w.parse_zipfile(path)
