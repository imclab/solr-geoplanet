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

    def __init__ (self, opts):

        self.opts = opts

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

        # self.parse_places(places)

        # self.parse_aliases(aliases)

        self.parse_adjacencies(adjacencies)

        if changes in file_list:
            self.parse_changes(changes)

        logging.info("finished parsing %s" % path)
        
    def parse_places(self, fname):

        logging.debug("parse places %s" % fname)

        reader = self.zf_reader(fname)
        docs = []

        counter = 0

        for row in reader:

            new = {
                'woeid': row['WOE_ID'],
                'parent_woeid': row['Parent_ID'],
                'name': row['Name'],
                'placetype': row['PlaceType'],
                'lang' : row['Language'],
                'iso': row['ISO'],
                'provider': 'geoplanet %s' % self.version
                }

            # pretty sure this is unnecessary - the datapacks
            # are meant to clobber older versions...

            # new = self.foo(new)

        docs.append(new)

        if len(docs) == 10000:
            counter += len(docs)
            solr.add(docs)
            docs = []
            
        if len(docs):
            counter += len(docs)
            solr.add(docs)

        logging.info("added %s docs" % counter)
        return True

    def parse_adjacencies(self, fname):

        logging.debug("parse adjacencies %s" % fname)

        reader = self.zf_reader(fname)
        docs = []
        new = {}

        counter = 0

        for row in reader:

            # TO DO: change the change for adjacent woeid
            # to be a dynamic adjacent_PLACETYPE field ?
            # do a lookup on adjacent_woeid (20130122/straup)
            
            # make adjacent_woeid a copy field...

            woeid = int(row['Place_WOE_ID'])
            adjacent_woeid = int(row['Neighbour_WOE_ID'])

            prev = new.get('woeid', False)

            # if this is a new woeid then submit updates

            if prev and prev != woeid:

                new = self._massage_adjacent(new)
                docs.append(new)

                new = {}

                if len(docs) == 10000:
                    logging.info("adjacencies counter @ %s" % counter)
                    counter += len(docs)
                    self.solr.add(docs)
                    docs = []

            # add the neighbour

            if new.get(woeid, False):
                new['adjacent_woeid'].append(adjacent_woeid)
            else:
                new = {'woeid' : woeid, 'adjacent_woeid' : [ adjacent_woeid  ] }

        # clean up any stragglers

        if len(new.keys()):
            new = self._massage_adjacent(new)
            docs.append(new)

        if len(docs):
            logging.info("adjacencies counter @ %s" % counter)
            counter += len(docs)
            self.solr.add(docs)

        logging.info("updated %s docs" % counter)

    def _massage_adjacent(self, new):

        old = self.get_by_woeid(new['woeid'])

        if old:

            del(old['date_indexed'])
            del(old['_version_'])

            adjacencies = old.get('adjacent_woeid', [])
                    
            for a in new['adjacent_woeid']:
                if not a in adjacencies:
                    adjacencies.append(a)

            new = old
            new['adjacent_woeid'] = adjacencies

        return new

    def parse_aliases(self, fname):

        logging.debug("parse aliases %s" % fname)

        reader = self.zf_reader(fname)
        docs = []
        new = {}

        counter = 0

        for row in reader:

            woeid = int(row['WOE_ID'])
            prev = new.get('woeid')

            # print "%s -> %s" % (woeid, prev)

            alias_k = "alias_%s_%s" % (row['Language'], row['Name_Type'])
            alias_v = row['Name']

            if prev and prev != woeid:

                # TO DO: account for existing aliases...
                new = self.foo(new)
                docs.append(new)

                new = {}

                if len(docs) == 10000:
                    logging.info("aliases counter @ %s" % counter)
                    counter += len(docs)
                    self.solr.add(docs)
                    docs = []

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
            logging.info("aliases counter @ %s" % counter)
            counter += len(docs)
            self.solr.add(docs)

        logging.info("updated %s docs" % counter)

    def _massage_aliases(self, new):

        old = self.get_by_woeid(new['woeid'])
    
        if old:
            pass

        return new

    def parse_changes(self, fname):
        pass

    def zf_reader(self, fname):

        fh = self.zf.open(fname)
        return unicodecsv.UnicodeReader(fh, delimiter='\t')

    def get_by_woeid(self, woeid):

        query = "woeid:%s" % woeid

        rsp = self.solr.search(q=query)

        if rsp.hits:
            return rsp.docs[0]

        else:
            return None

    # deprecated... (20130122/straup)

    def foo (self, new):

        old = self.get_by_woeid(new['woeid'])

        # TO DO: check version between old and new
        # if new > old then do not merge; new trumps
        # all ... I think (20130122/straup)

        if old:

            old = rsp.docs[0]
            del(old['date_indexed'])
            del(old['_version_'])

            new = dict(old.items() + new.items())

        new['provider'] = 'geoplanet %s' % self.version
        return new
        
if __name__ == '__main__':

    path = sys.argv[1]

    opts = None

    w = woedb(opts)
    w.parse_zipfile(path)
