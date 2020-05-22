import io
import json
import re
import traceback
from datetime import datetime

import dateutil.parser
import gridfs
import scrapy
from pymongo import MongoClient, HASHED
from scrapy import Request
from scrapy.http import JsonRequest

from ..pdf_extractor.paragraphs import extract_paragraphs_pdf_timeout


class PsyarxivSpider(scrapy.Spider):
    name = 'psyarxiv'
    url = 'https://share.osf.io/api/v2/search/creativeworks/_search'

    # DB specs
    db = None
    collection = None
    paper_fs = None
    collection_name = 'Scraper_share_osf_io'
    pdf_parser_version = 'psyarxiv_20200421'
    laparams = {
        'char_margin': 3.0,
        'line_margin': 2.5
    }
    post_params = {"query":{"bool":{"must":{"query_string":{"query":"*"}},"filter":[{"term":{"sources":"PsyArXiv"}},{"term":{"type":"preprint"}}]}},"from":0,"aggregations":{"sources":{"terms":{"field":"sources","size":500}}}}

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
        self.db = MongoClient('localhost', 27017).test_database
        self.collection = self.db[self.collection_name]

        # Create indices
        self.collection.create_index([('Doi', HASHED)])
        self.collection.create_index([('Title', HASHED)])
        self.collection.create_index('Publication_Date')

        # Grid FS
        self.paper_fs = gridfs.GridFS(self.db, collection=self.collection_name + '_fs')

    def start_requests(self):
        self.setup_db()

        yield JsonRequest(
            url=self.url,
            data=self.post_params,
            callback=self.get_num_papers,
        )

    def get_num_papers(self, response):
        data = json.loads(response.body)
        num_papers = data['hits']['total']
        num_iterations = num_papers - (num_papers % 10) # at most 10 papers per page

        for iteration in range(0, num_iterations + 1, 10):
            self.post_params['from'] = iteration
            yield JsonRequest(
                url=self.url,
                data=self.post_params,
                callback=self.parse_query_result,
            )

    def parse_query_result(self, response):
        data = json.loads(response.body)
        r_psyarxiv_link = re.compile(r'^http://psyarxiv.com/.*')

        for item in data['hits']['hits']:

            pubdate = dateutil.parser.isoparse(item['_source']['date_published'])
            psyarxiv_link = list(filter(r_psyarxiv_link.match, item['_source']['identifiers']))[0]
            psyarxiv_preprint_id = re.split('[ /]', psyarxiv_link)[3]

            if self.collection.find_one(
                    {'Title': item['_source']['title'], 'Publication_Date': pubdate}) is None:
                yield Request(
                    url='https://api.osf.io/v2/preprints/'+ psyarxiv_preprint_id + '/?format=json',
                    callback=self.parse_article,
                    meta={
                        'Title': item['_source']['title'],
                        'Journal': 'psyarxiv',
                        'Origin': 'All preprints from psyarxiv rss feed',
                        'Publication_Date': pubdate,
                        'Authors': [{'Name': x} for x in item['_source']['contributors']],
                        'Link': psyarxiv_link
                    }
                )

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
