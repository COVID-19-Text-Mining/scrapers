import io
import json
import re
import traceback
from datetime import datetime

import scrapy
from pymongo import MongoClient, HASHED
from scrapy import Request
from bs4 import BeautifulSoup

from ..html_extractor.paragraphs import extract_paragraphs_recursive


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

    def start_requests(self):
        self.setup_db()

        yield Request(
            url=self.url,
            callback=self.parse,
            dont_filter=True,
            meta={ 'dont_obey_robotstxt': True },
        )

    def parse(self, response):
        meta = response.meta
        links = response.xpath('//body//div[@class="articleTitle"]//a/@href').extract()
        publish_dates = response.xpath('//body//div[@class="published-online"]/text()').extract()

        for i in range(0, len(publish_dates)):
            publish_dates[i] = (re.search(r'^Published:\s(.*)$', publish_dates[i])).group(1)

        titles = [h2.xpath('string(a)').get() for h2 in response.xpath('//body//div[@class="articleTitle"]/h2')]

        for article_number in range(0, len(links)):
            if self.collection.find_one(
                    {'Title': titles[article_number], 'Publication_Date': publish_dates[article_number]}) is None:
                meta['Title'] = titles[article_number]
                meta['Journal'] = 'thelancet'
                meta['Origin'] = 'All coronavirus articles from thelancet'
                meta['Publication_Date'] = publish_dates[article_number]
                meta['Link'] = 'https://www.thelancet.com' + links[article_number]
                yield Request(
                    url='https://www.thelancet.com' + links[article_number],
                    callback=self.parse_article,
                    meta=meta
                )
        next_page = response.xpath('//body//li[@class="next"]/a/@href').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse, meta={ 'dont_obey_robotstxt': True})

    @staticmethod
    def find_text_html(content, title):
        # Parse the HTML
        paragraphs = extract_paragraphs_recursive(BeautifulSoup(content, features='html.parser'))

        def find_section(obj):
            if isinstance(obj, dict):
                if obj['name'] == title:
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

        text = find_section(paragraphs)
        if not isinstance(text, list):
            text = [text]
        return text

    def insert_article(self, article):
        meta_dict = {}
        for key in list(article):
            if key[0].islower():
                meta_dict[key] = article[key]
                del article[key]
        article['_scrapy_meta'] = meta_dict
        article['last_updated'] = datetime.now()

        self.collection.insert_one(article)

    def parse_article(self, response):
        meta = response.meta
        meta['Doi'] = response.xpath('//body//div[@class="inline-it"]//a/text()').extract_first()
        meta['Authors'] = [{'Name': x} for x in response.xpath('//body//li[@class="loa__item author"]/div[@class="dropBlock article-header__info"]/a/text()').extract()]
        meta['Text'] = self.find_text_html(response.text, meta['Title'])
        self.insert_article(meta)
