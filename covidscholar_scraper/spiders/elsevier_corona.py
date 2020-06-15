import io
import re
import time
from datetime import datetime

import pysftp
from pymongo import HASHED

from ._base import BaseSpider


class ElsevierCoronaSpider(BaseSpider):
    name = 'elsevier_corona'

    indices = [
        [('paper_id', HASHED)],
        'paper_id',
        'atime',
        'mtime',
    ]
    collections_config = {
        'Elsevier_corona_meta': indices,
        'Elsevier_corona_xml': indices
    }

    def handle_meta(self, fileattrs, connection):
        filename = fileattrs.filename
        atime = datetime.fromtimestamp(fileattrs.st_atime)
        mtime = datetime.fromtimestamp(fileattrs.st_mtime)
        m = re.match(r'([0-9a-zA-Z]+)_meta.json', filename)
        if not m:
            return
        paper_id = m.group(1)

        if self.has_duplicate(
                'Elsevier_corona_meta',
                {'paper_id': paper_id},
                lambda x: x['mtime'] >= mtime):
            return

        data = io.BytesIO()
        connection.getfo(filename, data)

        self.save_article(article={
            'paper_id': paper_id,
            'version': int(time.time()),
            'atime': atime,
            'mtime': mtime,
            'meta': data.getvalue().decode()
        }, to='Elsevier_corona_meta')

    def handle_xml(self, fileattrs, connection):
        filename = fileattrs.filename
        atime = datetime.fromtimestamp(fileattrs.st_atime)
        mtime = datetime.fromtimestamp(fileattrs.st_mtime)
        m = re.match(r'([0-9a-zA-Z]+).xml', filename)
        if not m:
            return
        paper_id = m.group(1)

        if self.has_duplicate(
                'Elsevier_corona_xml',
                {'paper_id': paper_id},
                lambda x: x['mtime'] >= mtime):
            return

        data = io.BytesIO()
        connection.getfo(filename, data)

        self.save_article(article={
            'paper_id': paper_id,
            'version': int(time.time()),
            'atime': atime,
            'mtime': mtime,
            'last_updated': mtime,
            'xml': data.getvalue().decode()
        }, to='Elsevier_corona_xml')

    def scrape_meta(self, connection):
        with connection.cd('meta'):
            last_checkin = 0
            for i, file in enumerate(connection.listdir_attr()):
                # Keep alive
                if time.time() > last_checkin + 5:
                    self.logger.info('Sending heart beat signal')
                    connection.stat(file.filename)
                    last_checkin = time.time()
                try:
                    self.handle_meta(file, connection)
                except Exception as e:
                    self.logger.exception('Cannot handle meta %r, %r', file, e)

    def scrape_xml(self, connection):
        with connection.cd('xml'):
            last_checkin = 0
            for file in connection.listdir_attr():
                # Keep alive
                if time.time() > last_checkin + 5:
                    self.logger.info('Sending heart beat signal')
                    connection.stat(file.filename)
                    last_checkin = time.time()
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
