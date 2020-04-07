import io
import re
from datetime import datetime

import pysftp
import scrapy
from pymongo import MongoClient, HASHED


class ElsevierCoronaSpider(scrapy.Spider):
    name = 'elsevier_corona'
    allowed_domains = ['semanticscholar.org']

    # DB specs
    db = None
    meta_collection = None
    xml_collection = None

    def setup_db(self):
        """Setup database and collection. Ensure indices."""
        self.db = MongoClient(
            host=self.settings['MONGO_HOSTNAME'],
        )[self.settings['MONGO_DB']]
        self.db.authenticate(
            name=self.settings['MONGO_USERNAME'],
            password=self.settings['MONGO_PASSWORD'],
            source=self.settings['MONGO_AUTHENTICATION_DB']
        )

        self.meta_collection = self.db['Elsevier_corona_meta']
        self.meta_collection.create_index([('paper_id', HASHED)])
        self.meta_collection.create_index('paper_id')
        self.meta_collection.create_index('atime')
        self.meta_collection.create_index('mtime')

        self.xml_collection = self.db['Elsevier_corona_xml']
        self.xml_collection.create_index([('paper_id', HASHED)])
        self.xml_collection.create_index('paper_id')
        self.xml_collection.create_index('atime')
        self.xml_collection.create_index('mtime')

    def handle_meta(self, fileattrs, connection):
        filename = fileattrs.filename
        atime = datetime.fromtimestamp(fileattrs.st_atime)
        mtime = datetime.fromtimestamp(fileattrs.st_mtime)
        m = re.match(r'([0-9a-zA-Z]+)_meta.json', filename)
        if not m:
            return
        paper_id = m.group(1)

        # check old files!
        old_meta = list(self.meta_collection.find({'paper_id': paper_id}, {'mtime': 1}))
        if any(x['mtime'] >= mtime for x in old_meta):
            return

        version = len(old_meta) + 1
        data = io.BytesIO()
        connection.getfo(filename, data)

        self.meta_collection.insert_one({
            'paper_id': paper_id,
            'version': version,
            'atime': atime,
            'mtime': mtime,
            'meta': data.getvalue().decode()
        })

    def handle_xml(self, fileattrs, connection):
        filename = fileattrs.filename
        atime = datetime.fromtimestamp(fileattrs.st_atime)
        mtime = datetime.fromtimestamp(fileattrs.st_mtime)
        m = re.match(r'([0-9a-zA-Z]+).xml', filename)
        if not m:
            return
        paper_id = m.group(1)

        # check old files!
        old_meta = list(self.xml_collection.find({'paper_id': paper_id}, {'mtime': 1}))
        if any(x['mtime'] >= mtime for x in old_meta):
            return

        version = len(old_meta) + 1
        data = io.BytesIO()
        connection.getfo(filename, data)

        self.xml_collection.insert_one({
            'paper_id': paper_id,
            'version': version,
            'atime': atime,
            'mtime': mtime,
            'last_updated': mtime,
            'xml': data.getvalue().decode()
        })

    def scrape_meta(self, connection):
        with connection.cd('meta'):
            for file in connection.listdir_attr():
                try:
                    self.handle_meta(file, connection)
                except Exception as e:
                    self.logger.exception('Cannot handle meta %r, %r', file, e)

    def scrape_xml(self, connection):
        with connection.cd('xml'):
            for file in connection.listdir_attr():
                try:
                    self.handle_xml(file, connection)
                except Exception as e:
                    self.logger.exception('Cannot handle XML %r, %r', file, e)

    def start_requests(self):
        self.setup_db()

        cnopts = pysftp.CnOpts()
        cnopts.hostkeys = None
        with pysftp.Connection(
                'coronacontent.np.elsst.com', cnopts=cnopts,
                username='public', password='beat_corona') as sftp:
            self.scrape_meta(sftp)
            self.scrape_xml(sftp)

        return ()
