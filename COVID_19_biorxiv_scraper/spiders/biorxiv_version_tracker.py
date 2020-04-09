import json
import logging
import re
from datetime import datetime
from urllib.parse import urljoin

import gridfs
import scrapy
from pymongo import MongoClient, HASHED
from scrapy import Request, Selector


class BiorxivVersionTrackerSpider(scrapy.Spider):
    name = 'biorxiv_version_tracker'
    allowed_domains = ['biorxiv.org', 'medrxiv.org']

    # DB specs
    db = None
    collection = None
    collection_name = 'Scraper_connect_biorxiv_org'
    tracker_collection = None
    tracker_collection_name = 'Scraper_connect_biorxiv_org_new_versions'

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
        self.collection = self.db[self.collection_name]
        self.tracker_collection = self.db[self.tracker_collection_name]

        self.tracker_collection.create_index([('Doi', HASHED)])

    def start_requests(self):
        self.setup_db()

        for document in self.collection.find():
            if self.tracker_collection.find_one({'Doi': document['Doi']}) is not None:
                continue
            yield Request(
                url=document['Link'] + '.article-info?versioned=true',
                callback=self.test_new_versions,
                meta=document)

    def test_new_versions(self, response):
        def url_to_doi(u):
            return '/'.join(u.split('?')[0].split('/')[-2:])

        version_urls = list(map(
            lambda x: urljoin(response.request.url, x),
            response.xpath('//div[contains(@class, "hw-versions")]//li/a/@href').extract()))
        versions = list(map(url_to_doi, version_urls))

        this_version = url_to_doi(response.meta['Link'])

        new_version_url = None

        for url, version in zip(version_urls, versions):
            if int(version.split('v')[-1]) > int(this_version.split('v')[-1]):
                new_version_url = url
                this_version = version

        if new_version_url is not None:
            site = 'biorxiv' if 'biorxiv.org' in new_version_url else 'medrxiv'
            self.logger.info('Registering new update job for DOI: %s Link: %s',
                             response.meta['Doi'], new_version_url)
            new_job = {
                'scrapy_url': new_version_url,
                'scrapy_site': site,
                'Doi': response.meta['Doi'],
                'Journal': site,
                'Publication_Date': response.meta['Publication_Date'],
                'Origin': response.meta['Origin']
            }
            self.tracker_collection.insert_one(new_job)
