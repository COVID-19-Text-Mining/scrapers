import io
import json
import re
import traceback
import urllib.parse
import zipfile
from datetime import datetime

import dateutil.parser
import gridfs
import scrapy
from PyPDF2.pdf import PdfFileReader, PdfFileWriter
from pymongo import MongoClient, HASHED
from scrapy import Request

from ..pdf_extractor.paragraphs import extract_paragraphs_pdf_timeout


def pdf_cat(input_files, output_stream):
    """https://stackoverflow.com/questions/3444645/merge-pdf-files"""
    input_streams = []
    try:
        # First open all the files, then produce the output file, and
        # finally close the input files. This is necessary because
        # the data isn't read from the input files until the write
        # operation. Thanks to
        # https://stackoverflow.com/questions/6773631/problem-with-closing-python-pypdf-writing-getting-a-valueerror-i-o-operation/6773733#6773733
        for input_file in input_files:
            input_streams.append(open(input_file, 'rb'))
        writer = PdfFileWriter()
        for reader in map(PdfFileReader, input_streams):
            for n in range(reader.getNumPages()):
                writer.addPage(reader.getPage(n))
        writer.write(output_stream)
    finally:
        for f in input_streams:
            f.close()


class ChemrxivSpider(scrapy.Spider):
    name = 'chemrxiv'
    allowed_domains = ['chemrxiv.org']
    start_urls = ['http://chemrxiv.org/']

    keyword = ':search_term:"COVID-19" OR ' \
              ':search_term:Coronavirus OR ' \
              ':search_term:"Corona virus" OR ' \
              ':search_term:"2019-nCoV" OR ' \
              ':search_term:"SARS-CoV" OR ' \
              ':search_term:"MERS-CoV" OR ' \
              ':search_term:"Severe Acute Respiratory Syndrome" OR ' \
              ':search_term:"Middle East Respiratory Syndrome"'
    # DB specs
    db = None
    collection = None
    paper_fs = None
    collection_name = 'Scraper_chemrxiv_org'
    pdf_parser_version = 'chemrxiv_20200421'
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

    def build_query_url(self, cursor=None):
        query_dict = {
            'types': '',
            'itemTypes': '',
            'licenses': '',
            'orderBy': 'relevant',
            'orderType': 'desc',
            'limit': 40,
            'search': self.keyword,
            'institutionId': 259,
        }
        if cursor:
            query_dict['cursor'] = cursor

        return 'https://chemrxiv.org/api/items?' + urllib.parse.urlencode(query_dict)

    def start_requests(self):
        self.setup_db()
        yield Request(
            url=self.build_query_url(),
            callback=self.parse_query_result,
        )

    def parse_query_result(self, response):
        data = json.loads(response.body)
        if 'cursor' in data:
            yield Request(
                url=self.build_query_url(cursor=data['cursor']),
                callback=self.parse_query_result,
                priority=100
            )

        for item in data['items']:
            # Only scrape articles
            if item['type'] != 'article':
                continue

            pubdate = dateutil.parser.isoparse(item['data']['publishedDate'])

            if self.collection.find_one(
                    {'Title': item['data']['title'], 'Publication_Date': pubdate}) is None:
                yield Request(
                    url=item['data']['publicUrl'],
                    callback=self.parse_article,
                    meta={
                        'Title': item['data']['title'],
                        'Journal': 'chemrxiv',
                        'Origin': 'chemrxiv scraper with keywords: COVID-19',
                        'Publication_Date': pubdate,
                        'Authors': [{'Name': x['name']} for x in item['data']['authors']]
                    }
                )

    def update_article(self, article, pdf_file=None):
        if pdf_file is not None:
            pdf_fn = article['Doi'].replace('/', '-') + '.pdf'

            # Remove old files
            for file in self.paper_fs.find(
                    {"filename": pdf_fn},
                    no_cursor_timeout=True):
                self.paper_fs.delete(file['_id'])

            pdf_data = pdf_file.read()
            parsing_result = self.parse_pdf(pdf_data, pdf_fn)
            meta = parsing_result.copy()
            meta.update({
                'filename': pdf_fn,
                'manager_collection': self.collection_name,
            })
            file_id = self.paper_fs.put(pdf_data, **meta)
            article['PDF_gridfs_id'] = file_id
        else:
            article['PDF_gridfs_id'] = None

        meta_dict = {}
        for key in list(article):
            if key[0].islower():
                meta_dict[key] = article[key]
                del article[key]
        article['_scrapy_meta'] = meta_dict
        article['last_updated'] = datetime.now()

        self.collection.update(
            {'Doi': article['Doi']},
            article,
            upsert=True
        )

    def handle_pdf(self, response):
        result = response.meta

        pdf_file = io.BytesIO(response.body)
        self.update_article(result, pdf_file=pdf_file)

    def handle_zip_or_pdf(self, response):
        if response.headers['Content-Type'] == b'application/pdf':
            return self.handle_pdf(response)
        elif response.headers['Content-Type'] == b'application/zip':
            f = io.BytesIO(response.body)
            z = zipfile.ZipFile(file=f)
            pdf_stream = []
            for name in z.namelist():
                if name.lower().endswith('.pdf'):
                    pdf_stream.append(z.open(name))

            combined = io.BytesIO()
            pdf_cat(pdf_stream, combined)
            combined.seek(0)

            self.update_article(response.meta, pdf_file=combined)
        else:
            self.update_article(response.meta)

    def parse_article(self, response):
        meta = response.meta
        meta['Link'] = response.request.url
        meta['Doi'] = response.xpath('//meta[@name="citation_doi"]/@content').extract_first()

        # Note that here we actually combines all paragraphs into one.
        # Maybe preferable now since we use ElasticSearch.
        # However maybe use other (bs4) to extract paragraphs.
        meta['Abstract'] = list(filter(
            lambda x: x.strip(),
            response.xpath('string(//div[contains(@class,"description")])').extract()))
        meta['Abstract'] = list(map(
            lambda x: re.sub(r'\s+', ' ', x),
            meta['Abstract']
        ))
        meta['Keywords'] = list(filter(
            lambda x: x.strip(),
            response.xpath('//div[contains(@class,"tags")]//span/text()').extract()))

        article_link = re.search(r'exportPdfDownloadUrl"\s*:\s*"([^"]+)"', response.text)
        if article_link is not None:
            article_link = article_link.group(1)
        else:
            article_link = re.findall(r'downloadUrl"\s*:\s*"([^"]+)"', response.text)
            print(article_link)
            if len(article_link):
                article_link = article_link[0]
            else:
                article_link = None

        if article_link is None:
            # No PDF
            self.update_article(meta)
        else:
            yield Request(
                url=article_link,
                meta=meta,
                callback=self.handle_zip_or_pdf,
                priority=10,
                dont_filter=True,
            )
