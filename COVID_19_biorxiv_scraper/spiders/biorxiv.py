import json
import logging
import re
from datetime import datetime

import gridfs
import scrapy
from pymongo import MongoClient, HASHED
from scrapy import Request, Selector


class BiorxivSpider(scrapy.Spider):
    name = 'biorxiv'
    allowed_domains = ['biorxiv.org', 'medrxiv.org']
    json_source = 'https://connect.biorxiv.org/relate/collection_json.php?grp=181'

    # DB specs
    db = None
    collection = None
    paper_fs = None
    collection_name = 'Scraper_connect_biorxiv_org'

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

        # Grid FS
        self.paper_fs = gridfs.GridFS(self.db, collection=self.collection_name + '_fs')

    def update_article(self, article):
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

    def start_requests(self):
        self.setup_db()

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
                url='https://doi.org/%s' % doi,
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

        # Remove old files
        for file in self.paper_fs.find(
                {"filename": pdf_fn},
                no_cursor_timeout=True):
            self.paper_fs.delete(file['_id'])

        file_id = self.paper_fs.put(
            response.body,
            filename=pdf_fn,
            manager_collection=self.collection_name,
        )
        result['PDF_gridfs_id'] = file_id

        self.update_article(result)

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
                fn, ln = content.strip().rsplit(' ', maxsplit=1)
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
