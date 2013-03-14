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
import sqlite3

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

            old = self.get_by_woeid(new['woeid'])

            if old:

                for prop in ('woeid_supersedes', 'woeid_superseded_by'):

                    if old.get(prop, False):
                        new[prop] = old[prop]

            new['provider'] = 'geoplanet %s' % self.version

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

    def parse_adjacencies(self, fname):

        logging.debug("parse adjacencies %s" % fname)

        dbfile = "adjacencies-%s" % self.version

        setup = [
            "CREATE TABLE geoplanet_adjacencies (woeid INTEGER, neighbour INTEGER)",
            "CREATE INDEX adjacencies_by_woeid ON geoplanet_adjacencies (woeid)"
            ]

        con, cur = self.sqlite_db(dbfile, setup)

        reader = self.zf_reader(fname)

        logging.info("sql-ized adjacencies start")

        for row in reader:

            woeid = int(row['Place_WOE_ID'])
            woeid_adjacent = int(row['Neighbour_WOE_ID'])

            sql = "INSERT INTO geoplanet_adjacencies (woeid, neighbour) VALUES (?,?)"
            cur.execute(sql, (woeid, woeid_adjacent))

            con.commit()

        logging.info("sql-ized adjacencies complete")

        #

        docs = []
        counter = 0

        ids = []

        res = cur.execute("""SELECT DISTINCT(woeid) FROM geoplanet_adjacencies""")

        for row in res:
            woeid = row[0]
            ids.append(woeid)
        
        for woeid in ids:

            sql = """SELECT * FROM geoplanet_adjacencies WHERE woeid=?"""
            a_res = cur.execute(sql, (woeid,))

            adjacent = []

            for a_row in a_res:
                woeid, neighbour = a_row
                adjacent.append(neighbour)

            logging.debug("got %s neighbours for WOE ID %s" % (len(adjacent), woeid))

            loc = self.get_by_woeid(woeid)

            # Blurgh...

            if not loc:

                loc = {
                    'woeid': woeid,
                    'provider': 'geoplanet %s' % self.version
                    }

            loc['woeid_adjacent'] = adjacent

            docs.append(loc)

            if len(docs) == self.update_count:
                logging.info("adjacencies counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info("adjacencies counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("finished importing adjacencies")
        os.unlink(dbfile)

    def parse_aliases(self, fname):

        logging.debug("parse aliases %s" % fname)

        reader = self.zf_reader(fname)

        logging.info("sql-ized aliases start")

        dbfile = "aliases-%s" % self.version

        setup = [
            "CREATE TABLE geoplanet_aliases (woeid INTEGER, name TEXT, type TEXT)",
            "CREATE INDEX aliases_by_woeid ON geoplanet_aliases (woeid)"
            ]

        con, cur = self.sqlite_db(dbfile, setup)

        for row in reader:

            woeid = int(row['WOE_ID'])
            name = row['Name']

            type = "%s_%s" % (row['Language'], row['Name_Type'])

            sql = "INSERT INTO geoplanet_aliases (woeid, name, type) VALUES (?,?,?)"
            cur.execute(sql, (woeid, name, type))

            con.commit()

        logging.info("sql-ized aliases complete")

        #

        docs = []
        counter = 0

        ids = []
        res = cur.execute("""SELECT DISTINCT(woeid) FROM geoplanet_aliases""")

        # ZOMGWTF... why do I need to do this????
        # (20130309/straup)

        for row in res:
            woeid = row[0]
            ids.append(woeid)

        for woeid in ids:

            sql = """SELECT * FROM geoplanet_aliases WHERE woeid=?"""
            a_res = cur.execute(sql, (woeid,))

            aliases = {}

            for a_row in a_res:

                woeid, name, type = a_row
                k = "alias_%s" % type

                names = aliases.get(k, [])
                names.append(name)

                aliases[k] = names

            loc = self.get_by_woeid(woeid)

            # Wot?!

            if not loc:

                loc = {
                    'woeid': woeid,
                    'provider': 'geoplanet %s' % self.version
                    }

            for k, v in aliases.items():
                loc[k] = v

            docs.append(loc)

            if len(docs) == self.update_count:
                logging.info("aliases counter @ %s" % counter)
                counter += len(docs)
                self._add(docs)
                docs = []

        if len(docs):
            logging.info("aliases counter @ %s" % counter)
            counter += len(docs)
            self._add(docs)

        logging.info("updated aliases for %s docs" % counter)
        os.unlink(dbfile)

    def parse_changes(self, fname):

        logging.info("parse changes %s" % fname)

        reader = self.zf_reader(fname)

        for row in reader:

            docs = []

            # I know right? This is a problem in the 
            # geoplanet_changes_7.8.1 file (20130313/straup)

            try:
                old_woeid = int(row['Woe_id'])
                new_woeid = int(row['Rep_id'])
            except Exception, e:
                print row
                continue

            old = self.get_by_woeid(old_woeid)
            new = self.get_by_woeid(new_woeid)

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
                logging.debug("new: %s supercedes: %s" % (new_woeid, supersedes))

                if new.get("woeid_supersede", False):
                    del(new["woeid_supersede"])

                if not old_woeid in supersedes:
                    supersedes.append(old_woeid)

                    new['woeid_supersedes'] = supersedes
                    docs.append(new)

            else:
                logging.warning("WTF... no record for new WOE ID (%s)" % new_woeid)

            if len(docs):
                self._add(docs, False)

        self.solr.optimize()

    def sqlite_db(self, dbfile, setup):

        if os.path.exists(dbfile):
            os.unlink(dbfile)

        con = sqlite3.connect(dbfile)
        cur = con.cursor()

        cur.execute("""PRAGMA synchronous=0""")
        cur.execute("""PRAGMA locking_mode=EXCLUSIVE""")
        cur.execute("""PRAGMA journal_mode=DELETE""")

        for cmd in setup:
            cur.execute(cmd)

        return con, cur

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
