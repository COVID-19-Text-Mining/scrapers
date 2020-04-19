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

        tar_gz_files = [
            (
                'comm_use_subset',
                'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/comm_use_subset.tar.gz'),
            (
                'noncomm_use_subset',
                'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/noncomm_use_subset.tar.gz'),
            (
                'custom_license',
                'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/custom_license.tar.gz'),
            (
                'biorxiv_medrxiv',
                'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/biorxiv_medrxiv.tar.gz'),
        ]
        for content_type, link in tar_gz_files:
            yield Request(
                url=link,
                callback=self.parse_gzip,
                meta={
                    'download_maxsize': 0,
                    'download_warnsize': 0,
                    'content_type': content_type,
                },
                dont_filter=True)

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

        content_type = response.meta['content_type']
        collection = self.db[self.subset_collection_map[content_type]]

        for file in archive.getmembers():
            path = file.name

            m = re.search(r'([\w_]+)[/\\].*?json', path)
            if not m:
                continue

            additional_annotation = m.group(1)

            contents = archive.extractfile(file)
            data = json.load(contents)
            paper_id = data['paper_id']

            insert = True

            old_doc = collection.find_one({'paper_id': data['paper_id']})
            if old_doc is not None:
                old_doc = {x: old_doc[x] for x in data}
                if old_doc == data:
                    insert = False

            if insert:
                self.logger.info("Insert paper with id %s", paper_id)
                data.update({
                    'last_updated': datetime.now(),
                    '_additional_flags': additional_annotation,
                })
                collection.insert_one(data)
