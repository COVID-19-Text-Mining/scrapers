import io
import json
import logging
import re
import traceback
from datetime import datetime

import gridfs
import scrapy
from pymongo import MongoClient, HASHED
from scrapy import Request, Selector

from ..pdf_extractor.paragraphs import extract_paragraphs_pdf_timeout


class BiorxivSpider(scrapy.Spider):
    name = 'biorxiv'
    allowed_domains = ['biorxiv.org', 'medrxiv.org']
    json_source = 'https://connect.biorxiv.org/relate/collection_json.php?grp=181'

    # DB specs
    db = None
    collection = None
    paper_fs = None
    collection_name = 'Scraper_connect_biorxiv_org'

    tracker_collection = None
    tracker_collection_name = 'Scraper_connect_biorxiv_org_new_versions'
    pdf_parser_version = 'biorxiv_20200421'
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
        self.collection.create_index('Publication_Date')

        self.tracker_collection = self.db[self.tracker_collection_name]

        # Grid FS
        self.paper_fs = gridfs.GridFS(self.db, collection=self.collection_name + '_fs')

    def insert_article(self, article):
        meta_dict = {}
        for key in list(article):
            if key[0].islower():
                meta_dict[key] = article[key]
                del article[key]
        article['_scrapy_meta'] = meta_dict
        article['last_updated'] = datetime.now()

        self.collection.insert_one(article)

    def start_requests(self):
        self.setup_db()

        # handle updated articles
        site_table = {
            'medrxiv': self.handle_medrxiv,
            'biorxiv': self.handle_biorxiv,
        }
        updates_to_remove = []
        for update in self.tracker_collection.find():
            yield Request(
                url=update['scrapy_url'],
                callback=site_table[update['scrapy_site']],
                meta={
                    'Doi': update['Doi'],
                    'Journal': update['Journal'],
                    'Publication_Date': update['Publication_Date'],
                    'Origin': update['Origin'],
                })
            updates_to_remove.append(update['_id'])

        for i in range(0, len(updates_to_remove), 100):
            end = min(len(updates_to_remove), 100 + i)
            self.tracker_collection.delete_many({'_id': {'$in': updates_to_remove[i:end]}})

        yield Request(
            url=self.json_source,
            callback=self.handle_article_list)

    def handle_article_list(self, response):
        site_table = {
            'medrxiv': self.handle_medrxiv,
            'biorxiv': self.handle_biorxiv,
        }

        data = json.loads(response.body_as_unicode())
        for entry in data['rels']:
            # DOI case insensitive
            doi = entry['rel_doi'].lower()
            publish_date = datetime.strptime(entry['rel_date'], '%Y-%m-%d')

            exists = self.collection.find_one(
                {'Doi': doi, 'Publication_Date': {'$gte': publish_date}}) is not None
            if exists:
                continue

            if entry['rel_site'] not in site_table:
                self.logger.log(
                    logging.CRITICAL,
                    "Unknown site %s", entry['rel_site'])
                continue

            yield Request(
                # url='https://doi.org/%s' % doi,
                url=entry['rel_link'],
                dont_filter=True,
                callback=site_table[entry['rel_site']],
                # Pass in initial meta data dictionary
                meta={
                    'Doi': doi,
                    'Journal': entry['rel_site'],
                    'Publication_Date': publish_date,
                    'Origin': 'COVID-19 SARS-CoV-2 preprints from medRxiv and bioRxiv @ %s' % self.json_source,
                })

    def handle_medrxiv_pdf(self, response):
        result = response.meta

        pdf_fn = result['Doi'].replace('/', '-') + '.pdf'

        # # Don't Remove old files
        # for file in self.paper_fs.find(
        #         {"filename": pdf_fn},
        #         no_cursor_timeout=True):
        #     self.paper_fs.delete(file._id)

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

    def handle_medrxiv(self, response):
        result = response.meta
        result['Link'] = response.request.url

        # Scrape title
        result['Title'] = response.xpath("//*[contains(@id,'page-title')]/text()").extract_first().strip()

        # Scrape author list
        authors = []
        for name, content in zip(
                response.xpath("//meta[contains(@name,'citation_author')]/@name").extract(),
                response.xpath("//meta[contains(@name,'citation_author')]/@content").extract()):
            if name == 'citation_author':
                # Split FN/LN by whitespace
                names = content.strip().rsplit(' ', maxsplit=1)
                if len(names) == 2:
                    fn, ln = names
                else:
                    fn, ln = '', names[0]
                authors.append({
                    'Name': {'fn': fn, 'ln': ln}
                })
            else:
                # Some values can be a list
                key = name[len('citation_author'):].strip('_').capitalize()
                if key not in authors[-1]:
                    authors[-1][key] = []
                authors[-1][key].append(content)
        result['Authors'] = authors

        # Abstract
        result['Abstract'] = []
        for p in response.xpath("//div[contains(@class, 'abstract') and contains(@class, 'section')]//p").extract():
            p = re.sub(r'\s+', ' ', ' '.join(Selector(text=p).xpath('//text()').extract()))
            result['Abstract'].append(p)

        # Subject area
        result['Subject_Area'] = list(filter(
            lambda x: len(x),
            map(
                lambda x: x.strip(),
                response.xpath("//div[contains(@class, 'pane-highwire-article-collections')]//li//text()").extract()
            )
        ))

        pdf_url = response.request.url + '.full.pdf'
        yield Request(
            url=pdf_url,
            meta=result,
            callback=self.handle_medrxiv_pdf,
            priority=10,
        )

    def handle_biorxiv(self, response):
        # They share the same page structure
        return self.handle_medrxiv(response)
