import io
import json
import re
import traceback
from datetime import datetime

import gridfs
import scrapy
from pymongo import MongoClient, HASHED
from scrapy import Request

from ..pdf_extractor.paragraphs import extract_paragraphs_pdf_timeout


class ThelancetSpider(scrapy.Spider):
    name = 'thelancet'
    url = 'https://www.thelancet.com/coronavirus/archive?startPage=0#navigation'

    # DB specs
    db = None
    collection = None
    paper_fs = None
    collection_name = 'Scraper_thelancet_com'
    pdf_parser_version = 'thelancet_20200421'
    laparams = {
        'char_margin': 3.0,
        'line_margin': 2.5
    }

    def parse_pdf(self, pdf_data, filename):
        data = io.BytesIO(pdf_data)
        try:
            paragraphs = extract_paragraphs_pdf_timeout(data, laparams=self.laparams, return_dicts=True)
            return {
                'pdf_extraction_success': True,
                'pdf_extraction_plist': paragraphs,
                'pdf_extraction_exec': None,
                'pdf_extraction_version': self.pdf_parser_version,
                'parsed_date': datetime.now(),
            }
        except Exception as e:
            self.logger.exception(f'Cannot parse pdf for file {filename}')
            exc = f'Failed to extract PDF {filename} {e}' + traceback.format_exc()
            return {
                'pdf_extraction_success': False,
                'pdf_extraction_plist': None,
                'pdf_extraction_exec': exc,
                'pdf_extraction_version': self.pdf_parser_version,
                'parsed_date': datetime.now(),
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
        self.collection = self.db[self.collection_name]

        # Create indices
        self.collection.create_index([('Doi', HASHED)])
        self.collection.create_index([('Title', HASHED)])
        self.collection.create_index('Publication_Date')

        # Grid FS
        self.paper_fs = gridfs.GridFS(self.db, collection=self.collection_name + '_fs')

    def start_requests(self):
        self.setup_db()

        yield Request(
            url=self.url,
            callback=self.parse,
            dont_filter=True,
        )

    def parse(self, response):
        links = response.xpath('//body//div[@class="articleTitle"]//a/@href').extract()
        publish_dates = response.xpath('//body//div[@class="published-online"]/text()').extract()

        for i in range(0, len(publish_dates)):
            publish_dates[i] = (re.search(r'^Published:\s(.*)$', publish_dates[i])).group(1)

        titles = [h2.xpath('string(a)').get() for h2 in response.xpath('//body//div[@class="articleTitle"]/h2')]

        for article_number in range(0, len(links)):
            if self.collection.find_one(
                    {'Title': titles[article_number], 'Publication_Date': publish_dates[article_number]}) is None:
                yield Request(
                    url='https://www.thelancet.com' + links[article_number],
                    callback=self.parse_article,
                    meta={
                        'Title': titles[article_number],
                        'Journal': 'thelancet',
                        'Origin': 'All coronavirus articles from thelancet',
                        'Publication_Date': publish_dates[article_number],
                        'Link': 'https://www.thelancet.com' + links[article_number],
                    }
                )
        next_page = response.xpath('//body//li[@class="next"]/a/@href').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse)

    def insert_article(self, article):
        meta_dict = {}
        for key in list(article):
            if key[0].islower():
                meta_dict[key] = article[key]
                del article[key]
        article['_scrapy_meta'] = meta_dict
        article['last_updated'] = datetime.now()

        self.collection.insert_one(article)

    def handle_pdf(self, response):
        result = response.meta
        pdf_fn = result['Doi'].replace('/', '-') + '.pdf'

        pdf_data = response.body
        parsing_result = self.parse_pdf(pdf_data, pdf_fn)
        meta = parsing_result.copy()
        meta.update({
            'filename': pdf_fn,
            'manager_collection': self.collection_name,
            'page_link': result['Link'],
        })
        file_id = self.paper_fs.put(pdf_data, **meta)

        result['PDF_gridfs_id'] = file_id

        self.insert_article(result)

    def parse_article(self, response):
        meta = response.meta
        meta['Doi'] = response.xpath('//body//div[@class="inline-it"]//a/text()').extract_first()
        article_link = response.xpath('//body//li[@class="article-tools__item article-tools__pdf"]/a/@href').get()

        yield Request(
            url='https://www.thelancet.com' + article_link,
            meta=meta,
            callback=self.handle_pdf,
            priority=10,
            dont_filter=True,
        )
