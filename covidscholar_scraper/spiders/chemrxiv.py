import io
import json
import re
import urllib.parse
import validators
import zipfile

import dateutil.parser
from PyPDF2.pdf import PdfFileReader, PdfFileWriter
from PyPDF2.utils import PdfReadError
from pymongo import HASHED
from scrapy import Request

from ._base import BaseSpider


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
            input_streams.append(input_file)
        writer = PdfFileWriter()
        for reader in map(PdfFileReader, input_streams):
            for n in range(reader.getNumPages()):
                writer.addPage(reader.getPage(n))
        writer.write(output_stream)
    finally:
        for f in input_streams:
            f.close()


def extract_zip_as_single_pdf(zip_data):
    f = io.BytesIO(zip_data)
    z = zipfile.ZipFile(file=f)
    pdf_stream = []
    for name in z.namelist():
        if name.lower().endswith('.pdf'):
            pdf_stream.append(io.BytesIO(z.open(name).read()))
    if not pdf_stream:
        return None

    combined = io.BytesIO()
    try:
        pdf_cat(pdf_stream, combined)
    except PdfReadError:
        return None

    combined.seek(0)

    return combined.read()


class ChemrxivSpider(BaseSpider):
    name = 'chemrxiv'
    allowed_domains = ['chemrxiv.org']

    keyword = ':search_term:"COVID-19" OR ' \
              ':search_term:Coronavirus OR ' \
              ':search_term:"Corona virus" OR ' \
              ':search_term:"2019-nCoV" OR ' \
              ':search_term:"SARS-CoV" OR ' \
              ':search_term:"MERS-CoV" OR ' \
              ':search_term:"Severe Acute Respiratory Syndrome" OR ' \
              ':search_term:"Middle East Respiratory Syndrome"'

    # DB specs
    collections_config = {
        'Scraper_chemrxiv_org': [
            [('Doi', HASHED)],
            [('Title', HASHED)],
            'Publication_Date',
        ],
    }
    gridfs_config = {
        'Scraper_chemrxiv_org_fs': [],
    }

    pdf_parser_version = 'chemrxiv_20200421'
    pdf_laparams = {
        'char_margin': 3.0,
        'line_margin': 2.5
    }

    def build_query_url(self, cursor=None):
        query_dict = {
            'types': '',
            'itemTypes': '',
            'licenses': '',
            'orderBy': 'published_date',
            'orderType': 'desc',
            'limit': 40,
            'search': self.keyword,
            'institutionId': 259,
        }
        if cursor:
            query_dict['cursor'] = cursor

        return 'https://chemrxiv.org/api/items?' + urllib.parse.urlencode(query_dict)

    def start_requests(self):
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

            if not self.has_duplicate(
                    'Scraper_chemrxiv_org',
                    {'Title': item['data']['title'], 'Publication_Date': pubdate}):
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

    def update_article(self, article, pdf_data=None, pdf_link=None):
        if pdf_data is not None:
            file_id = self.save_pdf(
                pdf_bytes=pdf_data,
                pdf_fn=article['Doi'].replace('/', '-') + '.pdf',
                pdf_link=pdf_link,
                fs='Scraper_chemrxiv_org_fs',
            )
        else:
            file_id = None

        article['PDF_gridfs_id'] = file_id
        self.save_article(article, to='Scraper_chemrxiv_org')

    def handle_zip_or_pdf(self, response):
        if response.headers['Content-Type'] == b'application/pdf':
            self.update_article(
                response.meta,
                pdf_data=response.body,
                pdf_link=response.request.url)
        elif response.headers['Content-Type'] == b'application/zip':
            self.update_article(
                response.meta,
                pdf_data=extract_zip_as_single_pdf(response.body),
                pdf_link=response.request.url)
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
            response.xpath('//meta[@name="citation_abstract"]/@content').extract()))
        meta['Abstract'] = list(map(
            lambda x: re.sub(r'\s+', ' ', x),
            meta['Abstract']
        ))
        meta['Keywords'] = list(filter(
            lambda x: x.strip(),
            response.xpath('//meta[@name="citation_keywords"]/@content').extract_first().split('; ')))

        article_id = meta['Link'].split('/')[-1]
        article_link = "https://chemrxiv.org/ndownloader/articles/{}/versions/1/export_pdf".format(article_id)

        if article_link and not validators.url(article_link):
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
