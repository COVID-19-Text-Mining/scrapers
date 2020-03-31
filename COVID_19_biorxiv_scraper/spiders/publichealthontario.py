# -*- coding: utf-8 -*-
import io
import re
from datetime import datetime

import gridfs
import scrapy
from pymongo import MongoClient, HASHED
from scrapy import Request
from scrapy import Selector


class PublichealthontarioSpider(scrapy.Spider):
    name = 'publichealthontario'
    allowed_domains = ['publichealthontario.ca']

    # DB specs
    db = None
    collection = None
    grid_fs = None

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
        self.collection = self.db['Scraper_publichealthontario']
        self.collection.create_index([('Link', HASHED)])
        self.collection.create_index('Date_Created')

        self.grid_fs = gridfs.GridFS(self.db, collection='Scraper_publichealthontario_fs')

    def start_requests(self):
        self.setup_db()

        yield Request(
            url='https://www.publichealthontario.ca/en/diseases-and-conditions/infectious-diseases/respiratory-diseases/novel-coronavirus/articles',
            callback=self.parse)

    def handle_pdf(self, response):
        result = response.meta

        pdf_file = io.BytesIO(response.body)
        file_id = self.grid_fs.put(
            pdf_file.read(),
            filename=re.sub(r'[^a-zA-Z0-9]', '-', result['Title']) + '.pdf',
        )
        result['PDF_gridfs_id'] = file_id

        meta_dict = {}
        for key in list(result):
            if key[0].islower():
                meta_dict[key] = result[key]
                del result[key]
        result['_scrapy_meta'] = meta_dict
        result['last_updated'] = datetime.now()

        self.collection.update(
            {'Link': result['Link']},
            result,
            upsert=True
        )

    def parse(self, response):
        for row in response.xpath('//table//tr').extract():
            date_created = Selector(text=row).xpath(
                '//div[contains(@class, "postDate")]/text()').extract_first()
            authors = Selector(text=row).xpath(
                '//td[2]/text()[1]').extract_first()
            title = Selector(text=row).xpath(
                '//td[2]/strong/text()').extract_first()
            journal = Selector(text=row).xpath(
                '//td[2]/text()[2]').extract_first()
            link = Selector(text=row).xpath(
                '//td[2]/a/@href').extract_first()
            desc = Selector(text=row).xpath(
                '//td[3]/text()').extract_first()
            synopsis = Selector(text=row).xpath(
                '//td[4]/a/@href').extract_first()

            old_item = self.collection.find_one({'Link': link})
            if synopsis is None or (old_item is not None and old_item['Date_Created'] >= date_created):
                continue

            meta = {
                'Date_Created': date_created,
                'Authors': authors.split(', '),
                'Title': re.sub(r'\s+', ' ', title),
                'Journal_String': re.sub(r'\s+', ' ', journal),
                'Link': link,
                'Desc': re.sub(r'\s+', ' ', desc),
            }
            yield response.follow(
                url=synopsis,
                callback=self.handle_pdf,
                dont_filter=True,
                meta=meta,
                priority=10,
            )
