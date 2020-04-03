# -*- coding: utf-8 -*-
import io
import re
from datetime import datetime

import gridfs
import scrapy
from bs4 import BeautifulSoup
from pymongo import MongoClient, HASHED
from scrapy import Request
from scrapy import Selector
from scrapy.http import TextResponse

from COVID_19_biorxiv_scraper.html_extractor.paragraphs import extract_paragraphs_recursive


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

    def save_object(self, result):
        pdf_bytes = result['pdf_bytes']
        del result['pdf_bytes']

        pdf_file = io.BytesIO(pdf_bytes)
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

    @staticmethod
    def find_abstract_by_parsing(content):
        # Parse the HTML
        paragraphs = extract_paragraphs_recursive(BeautifulSoup(content, features='html.parser'))

        def find_section(obj):
            if isinstance(obj, dict):
                if re.sub(r'[^\w]+', '', obj['name']).lower() == 'abstract':
                    return list(filter(lambda x: isinstance(x, str), obj['content']))
                elif isinstance(obj['content'], list):
                    for i in obj['content']:
                        r = find_section(i)
                        if r:
                            return r
            elif isinstance(obj, list):
                for i in obj:
                    r = find_section(i)
                    if r:
                        return r

            return []

        abstract = find_section(paragraphs)
        if not isinstance(abstract, list):
            abstract = [abstract]
        return abstract

    def handle_paper_page(self, response):
        meta = response.meta

        if isinstance(response, TextResponse):
            # Webpage parsing has a higher priority
            abstract = self.find_abstract_by_parsing(response.text)

            if len(abstract) == 0:
                # Try getting meta
                abstract = response.xpath('//meta[@name="citation_abstract"]/@content').extract()
                if len(abstract) == 0:
                    abstract = response.xpath('//meta[@name="dc.description"]/@content').extract()
                if len(abstract) == 0:
                    abstract = response.xpath('//meta[@name="description"]/@content').extract()
        else:
            abstract = None

        meta['Abstract'] = abstract
        self.save_object(meta)

    def handle_pdf(self, response):
        meta = response.meta
        meta['pdf_bytes'] = response.body
        meta['Synopsis_Link'] = response.request.url
        yield response.follow(
            url=meta['Link'],
            callback=self.handle_paper_page,
            dont_filter=True,
            meta=meta,
            priority=12,
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
