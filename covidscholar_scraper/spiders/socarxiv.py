import json
import re

import dateutil.parser
import scrapy
from pymongo import HASHED
from scrapy import Request
from scrapy.http import JsonRequest

from ._base import BaseSpider


class SocarxivSpider(BaseSpider):
    name = 'socarxiv'

    # DB specs
    collections_config = {
        'Scraper_share_osf_io_socarxiv': [
            [('Doi', HASHED)],
            [('Title', HASHED)],
            'Publication_Date',
        ],
    }
    gridfs_config = {
        'Scraper_share_osf_io_socarxiv_fs': [],
    }

    pdf_parser_version = 'socarxiv_20200421'
    pdf_laparams = {
        'char_margin': 3.0,
        'line_margin': 2.5
    }

    url = 'https://share.osf.io/api/v2/search/creativeworks/_search'

    post_params = {"query":{"bool":{"must":{"query_string":{"query":"title:covid-19 OR description:covid-19 OR title:coronavirus OR description:coronavirus OR title:SARS-CoV-2 OR description:SARS-CoV-2"}},
        "filter":[{"term":{"sources":"SocArXiv"}},
        {"term":{"type":"preprint"}}]}},"from":0,
        "aggregations":{"sources":{"terms":{"field":"sources","size":500}}}}

    def start_requests(self):
        yield JsonRequest(
            url=self.url,
            data=self.post_params,
            callback=self.get_num_papers,
            meta={ 'dont_obey_robotstxt': True }
        )

    def get_num_papers(self, response):
        meta = response.meta
        data = json.loads(response.body)
        num_papers = data['hits']['total']
        num_iterations = num_papers - (num_papers % 10) # at most 10 papers per page

        for iteration in range(0, num_iterations + 1, 10):
            self.post_params['from'] = iteration
            yield JsonRequest(
                url=self.url,
                data=self.post_params,
                callback=self.parse_query_result,
                meta=meta,
                dont_filter=True
            )

    def parse_query_result(self, response):
        meta = response.meta
        data = json.loads(response.body)
        r_socarxiv_link = re.compile(r'^http://osf.io/.*')

        for item in data['hits']['hits']:
            pubdate = dateutil.parser.isoparse(item['_source']['date_published'])
            try:
                socarxiv_link = list(filter(r_socarxiv_link.match, item['_source']['identifiers']))[0]
            except:
                continue
            socarxiv_preprint_id = re.split('[ /]', socarxiv_link)[3]

            if not self.has_duplicate(
                    'Scraper_share_osf_io_socarxiv',
                    {'Title': item['_source']['title'], 'Publication_Date': pubdate}):
                meta['Title'] = item['_source']['title']
                meta['Journal'] = 'socarxiv'
                meta['Origin'] = 'All preprints from socarxiv rss feed'
                meta['Publication_Date'] = pubdate
                meta['Authors'] = [{'Name': x} for x in item['_source']['contributors']]
                meta['Link'] = socarxiv_link
                yield Request(
                    url='https://api.osf.io/v2/preprints/'+ socarxiv_preprint_id + '/?format=json',
                    callback=self.parse_article,
                    meta=meta
                )

    def handle_pdf(self, response):
        pdf_data = response.body
        pdf_link = response.request.url
        article = response.meta
        if pdf_data is not None:
            file_id = self.save_pdf(
                pdf_bytes=pdf_data,
                pdf_fn=article['Doi'].replace('/', '-') + '.pdf',
                pdf_link=pdf_link,
                fs='Scraper_share_osf_io_socarxiv_fs',
            )
        else:
            file_id = None

        article['PDF_gridfs_id'] = file_id
        self.save_article(article, to='Scraper_share_osf_io_socarxiv')

    def parse_article(self, response):
        meta = response.meta
        preprint_data = json.loads(response.body)
        meta['Doi'] = preprint_data['data']['links']['preprint_doi']
        meta['Abstract'] = preprint_data['data']['attributes']['description']
        meta['Keywords'] = preprint_data['data']['attributes']['tags']
        article_link = meta['Link'] + 'download'

        yield Request(
            url=article_link,
            meta=meta,
            callback=self.handle_pdf,
            priority=10,
            dont_filter=True,
        )
