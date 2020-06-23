# -*- coding: utf-8 -*-
import datetime
import json
import re
from urllib.parse import urlencode

import scrapy
from pymongo import HASHED

from ._base import BaseSpider


class PatentSpider(BaseSpider):
    name = "lens_patent_spider"

    collections_config = {
        'Scraper_lens_patents': [
            [('Lens_ID', HASHED)],
            'Published_Date',
        ]
    }

    @staticmethod
    def build_lens_url(kwargs, from_i=0):
        base_url = 'https://www.lens.org/lens/api/search/patent'
        query_dict = {
            'q': '',
            'st': 'true',
            'collectionId': 179940,
            'e': 'false',
            'f': 'false',
            'l': 'en',
            'publishedDate.from': '2020-01-01',
            'publishedDate.to': '2020-06-23'
        }
        query_dict.update(kwargs)

        payload = json.dumps({
            "size": "50",
            "from": from_i,
            "sort": [
                {"pub_date": {"order": "desc"}}
            ]})
        return ('%s?%s' % (base_url, urlencode(query_dict))), payload

    def start_requests(self):
        today = datetime.datetime.now().strftime("%Y-%m-%d")
        three_mo = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")

        url, payload = self.build_lens_url({
            'publishedDate.from': three_mo,
            'publishedDate.to': today,
        })
        yield scrapy.Request(
            url=url,
            method='POST',
            headers={
                'content-type': 'application/json'
            },
            body=payload,
            callback=self.parse,
            meta={'from': 0},
            dont_filter=True)

    def parse(self, response):
        data = json.loads(response.text)

        for patent in data['hits']:
            publication_number = patent['displayKey']
            lens_id = patent['lensId']
            title = patent['titleFallbackToDisplayKey']

            published_date = datetime.datetime.fromtimestamp(patent['publicationDate'] / 1000)
            filed_date = datetime.datetime.fromtimestamp(patent['filingDate'] / 1000)
            earliest_priority_date = datetime.datetime.strptime(patent['earliestPriorityDate'], '%Y-%m-%d')
            applicants = list(map(str.capitalize, patent['applicants']))

            abstract_link = "https://www.lens.org/lens/patent/%s" % (lens_id,)

            if self.has_duplicate(
                    where='Scraper_lens_patents',
                    query={'Lens_ID': lens_id}):
                continue

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

                    "Returned_Data": patent,
                },
                priority=10,
            )

        if len(data['hits']) == 50:
            today = datetime.datetime.now().strftime("%Y-%m-%d")
            three_mo = (datetime.datetime.now() - datetime.timedelta(days=90)).strftime("%Y-%m-%d")

            url, payload = self.build_lens_url({
                'publishedDate.from': three_mo,
                'publishedDate.to': today,
            }, from_i=response.meta['from'] + 50)
            yield scrapy.Request(
                url=url,
                method='POST',
                headers={
                    'content-type': 'application/json'
                },
                body=payload,
                callback=self.parse,
                meta={'from': response.meta['from'] + 50},
                dont_filter=True)

    def parse_abstract(self, response):
        meta = response.meta

        abstract_text = json.loads(re.search(
            r'"abstract"\s*:({.*?\})', response.text).group(1))
        meta['Abstract'] = abstract_text['text']

        meta['HTML_Link'] = "https://www.lens.org/lens/patent/%s/fulltext" % (meta['Lens_ID'],)

        yield scrapy.Request(
            meta['HTML_Link'],
            callback=self.parse_full_text,
            meta=meta,
        )

    def parse_full_text(self, response):
        fulltext = response.xpath('.//div[@id="fullText"]').extract_first()
        meta = response.meta
        meta['Full_Text'] = fulltext
        meta['Last_Updated'] = datetime.datetime.now()
        self.save_article(meta, to='Scraper_lens_patents')
