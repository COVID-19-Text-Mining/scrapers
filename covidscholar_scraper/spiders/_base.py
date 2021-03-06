import io
import traceback
from datetime import datetime
from typing import Dict, Union, Optional

import gridfs
import scrapy
import sentry_sdk
from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from ..html_extractor.paragraphs import extract_paragraphs_recursive, get_tag_text
from ..pdf_extractor.paragraphs import extract_paragraphs_pdf_timeout


class BaseSpider(scrapy.Spider):
    # PDF parsing LA Params.
    pdf_laparams = None

    def __init__(self, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)

        # Note: these are empty when initialized. They will be populated
        # once any call to get_col or get_gridfs is called.
        self.db: Database = None
        self.collections: Dict[str, Collection] = {}
        self.gridfs: Dict[str, gridfs.GridFS] = {}

    def closed(self, reason):
        if self.settings['SENTRY_DSN']:
            with sentry_sdk.push_scope() as scope:
                scope.set_extra("reason", reason)
                sentry_sdk.capture_message('Scraper %s finished' % (self.name,))

            sentry_sdk.flush()

    @property
    def collections_config(self) -> dict:
        """
        Returns a dictionary where the keys are collection names
        and values are lists of indices to create.
        """
        raise NotImplementedError

    @property
    def gridfs_config(self) -> dict:
        """
        Returns a dictionary where the keys are gridfs names
        and values are lists of indices to create (for the .files collection).
        """
        raise NotImplementedError

    @property
    def pdf_parser_version(self) -> float:
        """
        Returns a floating point representation as PDF version.
        """
        raise NotImplementedError

    def parse_pdf(self, pdf_data, filename):
        data = io.BytesIO(pdf_data)
        try:
            paragraphs = extract_paragraphs_pdf_timeout(
                data, laparams=self.pdf_laparams, return_dicts=True)
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

    @staticmethod
    def get_all_text_html(html_string):
        soup = BeautifulSoup(html_string, features='html.parser')
        return get_tag_text(soup)

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

    def save_article(self, article: dict, to: Union[Collection, str], push_lowercase_to_meta=True):
        """
        Save a processed article. Capitalized fields will be saved as is.
        Others will be treated as scrapy meta.

        :param article: The processed article item.
        :param to: The collection to save to.
        :param push_lowercase_to_meta: Whether the lower case keys should be pushed into meta document.
        :return:
        """
        if push_lowercase_to_meta:
            meta_dict = {}
            for key in list(article):
                if key[0].islower():
                    meta_dict[key] = article[key]
                    del article[key]
            article['_scrapy_meta'] = meta_dict

        article['last_updated'] = datetime.now()

        result = self.get_col(to).insert_one(article)

        if self.settings['SENTRY_DSN']:
            with sentry_sdk.push_scope() as scope:
                scope.set_extra('item_id', str(result.inserted_id))
                sentry_sdk.capture_message('Scraper %s: Article download' % (self.name,))
            sentry_sdk.flush()

    def save_pdf(self, pdf_bytes, pdf_fn, pdf_link, fs: Union[gridfs.GridFS, str]):
        """
        Process PDF bytes and save it into a GridFS collection.

        :param pdf_bytes: Bytes data of PDF file.
        :param pdf_fn: PDF filename.
        :param pdf_link: Link to PDF file.
        :param fs: GridFS in which PDF files are saved, or a name.
        :return: The ObjectId for this object in the GridFS.
        """
        parsing_result = self.parse_pdf(pdf_bytes, pdf_fn)
        meta = parsing_result.copy()
        meta.update({
            'filename': pdf_fn,
            'page_link': pdf_link,
        })
        file_id = self.get_gridfs(fs).put(pdf_bytes, **meta)

        if self.settings['SENTRY_DSN']:
            with sentry_sdk.push_scope() as scope:
                scope.set_extra('filename', pdf_fn)
                scope.set_extra('page_link', pdf_link)
                scope.set_extra('file_id', str(file_id))
                sentry_sdk.capture_message('Scraper %s: PDF download' % (self.name,))
            sentry_sdk.flush()

        return file_id

    def has_duplicate(self, where: Union[Collection, str], query, comparator: Optional[callable] = None) -> bool:
        """
        Check for duplicate items using a query.
        If any returned result matches the comparator, return True.
        If the comparator is None, just check for the existence of the document.

        :param where: The collection to check in.
        :param query: The mongo query to make.
        :param comparator: The comparator for documents.
        """
        col = self.get_col(where)

        results = col.find(query)
        if results.count() == 0:
            return False

        if comparator is None:
            return results.count() > 0

        return any(map(comparator, results))

    def get_col(self, name):
        self.setup_db()
        if isinstance(name, Collection):
            return name
        return self.collections[name]

    def get_gridfs(self, name):
        self.setup_db()
        if isinstance(name, gridfs.GridFS):
            return name
        return self.gridfs[name]

    def setup_db(self):
        """Setup database and collection. Ensure indices."""

        if self.db is not None:
            return

        self.db = MongoClient(
            host=self.settings['MONGO_HOSTNAME'],
        )[self.settings['MONGO_DB']]
        self.db.authenticate(
            name=self.settings['MONGO_USERNAME'],
            password=self.settings['MONGO_PASSWORD'],
            source=self.settings['MONGO_AUTHENTICATION_DB']
        )

        def create_index(col, inds):
            for index in inds:
                if not isinstance(index, tuple):
                    index = (index,)

                col.create_index(*index)

        for name, indices in self.collections_config.items():
            self.collections[name] = self.db[name]

            create_index(self.collections[name], indices)

        try:
            for name, indices in self.gridfs_config.items():
                self.gridfs[name] = gridfs.GridFS(self.db, collection=name)

                create_index(getattr(self.gridfs[name], '_GridFS__files'), indices)
        except NotImplementedError:
            pass
