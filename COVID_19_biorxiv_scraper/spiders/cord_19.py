import gzip
import io
import json
import re
import tarfile
from datetime import datetime

import scrapy
from pymongo import MongoClient
from scrapy import Request, Selector


class Cord19Spider(scrapy.Spider):
    name = 'cord_19'
    allowed_domains = ['semanticscholar.org']

    # DB specs
    db = None
    subset_collection_map = {
        'comm_use_subset': 'CORD_comm_use_subset',
        'noncomm_use_subset': 'CORD_noncomm_use_subset',
        'biorxiv_medrxiv': 'CORD_biorxiv_medrxiv',
        'custom_license': 'CORD_custom_license'
    }

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

    def start_requests(self):
        self.setup_db()

        yield Request(
            url='https://pages.semanticscholar.org/coronavirus-research',
            callback=self.parse_page
        )

    def parse_page(self, response):
        file_list_html = response.xpath('//ul').extract_first()
        for url in Selector(text=file_list_html).xpath('//a/@href').extract():
            if url.endswith('.tar.gz'):
                yield Request(
                    url=url,
                    callback=self.parse_gzip,
                    meta={'download_maxsize': 0, 'download_warnsize': 0},
                    dont_filter=True
                )

    def parse_gzip(self, response):
        fileio = io.BytesIO(response.body)
        gzipfile = gzip.GzipFile(fileobj=fileio, mode='rb')
        archive = tarfile.TarFile(fileobj=gzipfile, mode='r')

        for file in archive.getmembers():
            path = file.name

            m = re.search(r'([\w_]+)[/\\]([a-f0-9]+)\.json', path)
            if not m:
                continue

            content_type, paper_id = m.groups()
            if content_type not in self.subset_collection_map:
                continue

            contents = archive.extractfile(file)
            data = json.load(contents)

            collection = self.db[self.subset_collection_map[content_type]]

            insert = True

            old_doc = collection.find_one({'paper_id': data['paper_id']})
            if old_doc is not None:
                old_doc = {x: old_doc[x] for x in data}

                if old_doc == data:
                    insert = False

            if insert:
                self.logger.info("Insert paper with id %s", paper_id)
                data.update({
                    'last_updated': datetime.now()
                })
                collection.insert_one(data)
