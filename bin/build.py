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
import StringIO

class woedb:

    def __init__ (self, opts):

        self.opts = opts

        self.solr = pysolr.Solr(self.opts.solr)

        self.version = None
        self.zf = None

        self.update_count = 100000

    def purge(self):
        logging.info("purging the database...")
        self.solr.delete(q='*:*')
        self.solr.optimize()

    def _add(self, docs, optimize=True):
        self.solr.add(docs)

        if optimize:
            self.solr.optimize()

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
                }

            new = self._massage_place(new)

            docs.append(new)

            if len(docs) == self.update_count:
                logging.info("places %s counter @ %s" % (self.version, counter))
                counter += len(docs)

                self._add(docs)
                docs = []
            
        if len(docs):
            logging.info("places %s counter @ %s" % (self.version, counter))
            counter += len(docs)

            self._add(docs)

        logging.info("places %s added %s docs" % (self.version, counter))
        return True

    def _massage_place(self, new):

        old = self.get_by_woeid(new['woeid'])

        if old:

            for prop in ('woeid_supersedes', 'woeid_superseded_by'):

                if old.get(prop, False):
                    new[prop] = old[prop]

        new['provider'] = 'geoplanet %s' % self.version
        return new

    def parse_adjacencies(self, fname):

        logging.debug("parse adjacencies %s" % fname)

        reader = self.zf_reader(fname)
        docs = []

        last_woeid = None
        adjacent = []

        counter = 0

        for row in reader:

            woeid = int(row['Place_WOE_ID'])
            woeid_adjacent = int(row['Neighbour_WOE_ID'])

            str_adjacent = ",".join(map(str, adjacent))
            logging.debug("woeid: %s last: %s adjacent: %s" % (woeid, last_woeid, str_adjacent))

            if last_woeid and last_woeid != woeid:

                new = self._massage_adjacent(last_woeid, adjacent)
                docs.append(new)

                adjacent = []

            last_woeid = woeid
            adjacent.append(woeid_adjacent)

            if len(docs) == self.update_count:
                logging.info("adjacencies counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        # clean up any stragglers

        if len(adjacent):
            new = self._massage_adjacent(last_woeid, adjacent)
            docs.append(new)

        if len(docs):
            logging.info("adjacencies counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("updated %s docs" % counter)

    def _massage_adjacent(self, woeid, adjacent):

        loc = self.get_by_woeid(woeid)

        # TO DO: change the change for adjacent woeid
        # to be a dynamic adjacent_PLACETYPE field ?
        # do a lookup on woeid_adjacent (20130122/straup)
            
        # loc should always be defined but sometimes it
        # isn't because ... puppies? (20130217/straup)

        if not loc:

            loc = {
                'woeid': woeid,
                'woeid_adjacent': adjacent
                }

        else:

            del(loc['date_indexed'])
            del(loc['_version_'])

            already_adjacent = loc.get('woeid_adjacent', [])
                    
            for a in adjacent:
                if not a in already_adjacent:
                    already_adjacent.append(a)

            loc['woeid_adjacent'] = already_adjacent

        loc['provider'] = 'geoplanet %s' % self.version
        return loc

    def parse_aliases(self, fname):

        logging.debug("parse aliases %s" % fname)

        reader = self.zf_reader(fname)
        docs = []

        last_woeid = None
        aliases = {}

        counter = 0

        for row in reader:

            woeid = int(row['WOE_ID'])

            alias_k = "alias_%s_%s" % (row['Language'], row['Name_Type'])
            alias_v = row['Name']

            # logging.debug("woeid: %s last: %s row: %s" % (woeid, last_woeid, row))
            logging.debug("woeid: %s last: %s aliases: %s" % (woeid, last_woeid, aliases))

            if last_woeid and last_woeid != woeid:

                new = self._massage_aliases(last_woeid, aliases)
                docs.append(new)

                aliases = {}

            if aliases.get(alias_k):
                aliases[ alias_k ].append(alias_v)
            else:
                aliases[ alias_k ] = [ alias_v ]

            last_woeid = woeid

            if len(docs) == self.update_count:
                logging.info("aliases counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        #

        if len(aliases.keys()):
            new = self._massage_aliases(last_woeid, aliases)
            docs.append(new)

        if len(docs):
            logging.info("aliases counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("updated aliases for %s docs" % counter)

    def _massage_aliases(self, woeid, aliases):

        loc = self.get_by_woeid(woeid)
    
        if not loc:
            
            loc = {
                'woeid' :  woeid
                }

            for k, v in aliases:
                loc[k] = v

        else:

            del(loc['date_indexed'])
            del(loc['_version_'])

            aliases = {}

            # HOW: to account for aliases from earlier data releases?

            for k, v in aliases.items():

                if not loc.get(k):
                    loc[k] = v
                else:

                    for name in v:
                        if not v in loc[k]:
                            loc[k].append(name)

        loc['provider'] = 'geoplanet %s' % self.version
        return loc

    def parse_changes(self, fname):

        logging.debug("parse aliases %s" % fname)
        logging.warning("this has not been tested yet...")

        reader = self.zf_reader(fname)

        for row in reader:

            docs = []

            old_woeid = int(row['Woe_id'])
            new_woeid = int(row['Rep_id'])

            print "old: %s new: %s" % (old_woeid, new_woeid)

            old = self.get_by_woeid(old_woeid)
            new = self.get_by_woeid(new_woeid)

            print old
            print new

            if old:

                del(old['date_indexed'])
                del(old['_version_'])

                old['woeid_superseded_by'] = new_woeid
                old['provider'] = 'geoplanet %s' % self.version

                logging.debug("old: %s new: %s" % (old_woeid, new_woeid))

                docs.append(old)

            else:

                old = {}
                old['woeid'] = old_woeid
                old['woeid_superseded_by'] = new_woeid
                old['provider'] = 'geoplanet %s' % self.version
                docs.append(old)

            if new:
                del(new['date_indexed'])
                del(new['_version_'])

                supersedes = new.get('woeid_supersedes', [])
                print "%s supersedes: %s" % (new_woeid, supersedes)

                # logging.debug("new: %s supercedes: %s" % (new_woeid, supersedes))

                if new.get("woeid_supersede", False):
                    del(new["woeid_supersede"])

                if not old_woeid in supersedes:
                    supersedes.append(old_woeid)

                    new['woeid_supersedes'] = supersedes
                    docs.append(new)

            else:
                logging.warning("WTF... no record for new WOE ID (%s)" % new_woeid)

            print "docs: %s" % len(docs)

            print "---"

            if len(docs):
                self._add(docs, False)

        self.solr.optimize()

    def zf_reader(self, fname, delimiter='\t'):

        fh = self.zf.open(fname)

        # gggggrnnhhhnnnhnhn.... yes, really.

        known_bad = ('7.4.0', '7.4.1')

        if fname.startswith('geoplanet_changes') and self.version in known_bad:
            
            first = fh.next()

            out = StringIO.StringIO()
            out.write("\t".join(["Woe_id","Rep_id","Data_Version"]) + "\n")

            while fh.readable():
                try:
                    out.write(fh.next())
                except Exception, e:
                    break

            out.seek(0)

            return unicodecsv.UnicodeReader(out, delimiter=delimiter)

        else:
            return unicodecsv.UnicodeReader(fh, delimiter=delimiter)

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

    parser.add_option("--purge", dest="purge", action="store_true", help="...", default=False)

    (opts, args) = parser.parse_args()

    if opts.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    w = woedb(opts)

    if opts.purge:
        w.purge()

    for path in args:
        logging.info("processing %s" %path)
        w.parse_zipfile(path)
