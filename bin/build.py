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

        self.solr = pysolr.Solr(self.opts.solr)

        self.version = None
        self.zf = None

        self.update_count = 100000

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
                'woeid_parent': row['Parent_ID'],
                'name': row['Name'],
                'placetype': row['PlaceType'],
                'lang' : row['Language'],
                'iso': row['ISO'],
                'provider': 'geoplanet %s' % self.version
                }

            docs.append(new)

            if len(docs) == self.update_count:
                logging.info("places %s counter @ %s" % (self.version, counter))
                counter += len(docs)
                self.solr.add(docs)
                docs = []
            
        if len(docs):
            logging.info("places %s counter @ %s" % (self.version, counter))
            counter += len(docs)
            self.solr.add(docs)

        logging.info("places %s added %s docs" % (self.version, counter))
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
            # do a lookup on woeid_adjacent (20130122/straup)
            
            # make woeid_adjacent a copy field...

            woeid = int(row['Place_WOE_ID'])
            woeid_adjacent = int(row['Neighbour_WOE_ID'])

            prev = new.get('woeid', False)

            # if this is a new woeid then submit updates

            if prev and prev != woeid:

                new = self._massage_adjacent(new)
                docs.append(new)

                new = {}

                if len(docs) == self.update_count:
                    logging.info("adjacencies counter @ %s" % counter)
                    counter += len(docs)
                    self.solr.add(docs)
                    docs = []

            # add the neighbour

            if new.get(woeid, False):
                new['woeid_adjacent'].append(woeid_adjacent)
            else:
                new = {'woeid' : woeid, 'woeid_adjacent' : [ woeid_adjacent  ] }

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

            adjacencies = old.get('woeid_adjacent', [])
                    
            for a in new['woeid_adjacent']:
                if not a in adjacencies:
                    adjacencies.append(a)

            new = old
            new['woeid_adjacent'] = adjacencies

        new['provider'] = 'geoplanet %s' % self.version
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

                new = self._massage_aliases(new)
                docs.append(new)

                new = {}

                if len(docs) == self.update_count:
                    logging.info("aliases counter @ %s" % counter)
                    counter += len(docs)
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
            new = self._massage_aliases(new)
            docs.append(new)

        if len(docs):
            logging.info("aliases counter @ %s" % counter)
            counter += len(docs)
            self.solr.add(docs)

        logging.info("updated aliases for %s docs" % counter)

    def _massage_aliases(self, new):

        old = self.get_by_woeid(new['woeid'])
    
        if old:

            del(old['date_indexed'])
            del(old['_version_'])

            aliases = {}

            for k, v in new.items():

                if not k.startswith('alias'):
                    continue

                if old.get(k, False):
                    
                    aliases[k] = old[k]

                    for a in new[k]:
                        if not a in aliases[k]:
                            aliases[k].append(a)

                else:
                    aliases[k] = v

            new = dict(old.items() + aliases.items())

        new['provider'] = 'geoplanet %s' % self.version
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
        
if __name__ == '__main__':

    import optparse

    parser = optparse.OptionParser("""build.py --options geoplanet_data_X.Y.Z.zip""")
    parser.add_option("-s", "--solr", dest="solr", help="your solr endpoint; default is http://localhost:8983/solr/woedb", default='http://localhost:8983/solr/woedb')
    parser.add_option("-v", "--verbose", dest="verbose", action="store_true", help="enable chatty logging; default is false", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    path = args[0]
    print path

    w = woedb(opts)
    w.parse_zipfile(path)
