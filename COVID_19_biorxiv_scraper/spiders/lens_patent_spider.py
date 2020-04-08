# -*- coding: utf-8 -*-
import datetime
from urllib.parse import urlencode, urljoin

import scrapy
from pymongo import MongoClient, HASHED
from scrapy.utils.markup import remove_tags


class PatentSpider(scrapy.Spider):
    name = "lens_patent_spider"

    db = None
    collection_name = 'Scraper_lens_patents'
    collection = None

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
        self.collection.create_index([('Lens_ID', HASHED)])
        self.collection.create_index('Published_Date')

    @staticmethod
    def build_lens_url(**kwargs):
        base_url = 'https://www.lens.org/lens/search/collection/179940'
        query_dict = {
            'dates': '+pub_date:19740101-20200402',  # dates
            'l': 'en',  # language
            'st': 'true',
            'n': 50,
            'p': 0,
            'v': 'table',
            's': 'pub_date',  # sort by
            'd': '+'
        }
        query_dict.update(kwargs)
        return '%s?%s' % (base_url, urlencode(query_dict))

    def insert_patent(self, patent):
        meta_dict = {}
        for key in list(patent):
            if key[0].islower():
                meta_dict[key] = patent[key]
                del patent[key]
        patent['_scrapy_meta'] = meta_dict
        patent['Last_Updated'] = datetime.datetime.now()

        self.collection.update(
            {'Lens_ID': patent['Lens_ID']},
            patent,
            upsert=True
        )

    def start_requests(self):
        self.setup_db()

        yield scrapy.Request(
            url=self.build_lens_url(dates="+pub_date:19740101-20200402"),
            callback=self.parse,
            dont_filter=True)

    def parse(self, response):
        patents_this_page = response.xpath('//*[contains(@class, "div-table-results-row")]')
        for patent in patents_this_page:
            publication_number = ''.join(
                patent.xpath(
                    './/div[contains(@class, "doc-type")]//a/text()').extract()).strip()

            lens_id = ''.join(
                patent.xpath(
                    './/div[contains(@class, "lens-id")]//a/text()').extract()).strip()

            exists = self.collection.find_one(
                {'Lens_ID': lens_id}) is not None
            if exists:
                continue

            title = patent.xpath('.//h3//a/text()').extract_first().strip()

            published_data_raw = {}
            for entry in patent.xpath('.//ul[contains(@class, "header-meta")]/li'):
                key = entry.xpath('.//b/text()').extract_first().strip()
                key = key.strip(':')
                if not key:
                    continue

                value = ''.join(entry.xpath('./text()').extract()).strip()
                published_data_raw[key] = value

            try:
                published_date = datetime.datetime.strptime(
                    published_data_raw['Published'].replace(',', ''), '%b %d %Y')
            except (KeyError, ValueError):
                published_date = None

            try:
                filed_date = datetime.datetime.strptime(
                    published_data_raw['Filed'].replace(',', ''), '%b %d %Y')
            except (KeyError, ValueError):
                filed_date = None

            try:
                earliest_priority_date = datetime.datetime.strptime(
                    published_data_raw['Earliest Priority'].replace(',', ''), '%b %d %Y')
            except (KeyError, ValueError):
                earliest_priority_date = None

            applicants = published_data_raw.get('Applicant', [])
            if applicants:
                applicants = list(map(str.strip, applicants.split(',')))

            abstract_link = "https://www.lens.org/lens/patent/%s" % (lens_id,)
            yield scrapy.Request(
                abstract_link,
                callback=self.parse_abstract,
                meta={
                    "Title": title,
                    "Publication_Number": publication_number,
                    "Lens_ID": lens_id,
                    "Link": abstract_link,
                    "Applicants": applicants,

                    "Published_Date": published_date,
                    "Filed_Date": filed_date,
                    "Earliest_Priority_Date": earliest_priority_date,
                },
                priority=10,
            )

        if len(patents_this_page) == 50:
            next_page_extend_link = response.xpath(
                './/a[contains(@class, "fa-chevron-right")]/@href')
            if len(next_page_extend_link) > 0:
                yield scrapy.Request(
                    urljoin(
                        response.request.url,
                        next_page_extend_link.extract_first().strip()),
                    callback=self.parse
                )
            else:
                old_date = published_date.strftime("%Y%m%d")
                today = datetime.datetime.now().strftime("%Y%m%d")
                new_link = self.build_lens_url(
                    dates="+pub_date:{old}-{today}".format(
                        old=old_date, today=today))
                yield scrapy.Request(
                    new_link,
                    callback=self.parse
                )

    def parse_abstract(self, response):
        meta = response.meta

        abstract_text = remove_tags(response.xpath(
            './/div[@class="page-title"]/following-sibling::p[1]').extract_first().strip()).strip()
        meta['Abstract'] = abstract_text

        links = response.xpath(".//a/@href").extract()
        html_link = None

        for link in links:
            if "fulltext" in link:
                html_link = "https://www.lens.org" + link
                break
        meta['HTML_Link'] = html_link

        if html_link is not None:
            yield scrapy.Request(
                html_link,
                callback=self.parse_full_text,
                meta=meta,
            )
        else:
            self.insert_patent(meta)

    def parse_full_text(self, response):
        fulltext = response.xpath('.//div[@id="fullText"]').extract_first().strip()
        meta = response.meta
        meta['Full_Text'] = fulltext

        self.insert_patent(meta)
