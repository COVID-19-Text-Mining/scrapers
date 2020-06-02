import io
import traceback
from datetime import datetime
from typing import Dict, Union, Optional

import gridfs
import scrapy
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from ..pdf_extractor.paragraphs import extract_paragraphs_pdf_timeout


class BaseSpider(scrapy.Spider):
    # PDF parsing LA Params.
    pdf_laparams = None

    def __init__(self, *args, **kwargs):
        super(BaseSpider, self).__init__(*args, **kwargs)

        self.db: Database = None
        self.collections: Dict[str, Collection] = {}
        self.gridfs: Dict[str, gridfs.GridFS] = {}
        self.setup_db()

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

    def save_article(self, article: dict, to: Union[Collection, str]):
        """
        Save a processed article. Capitalized fields will be saved as is.
        Others will be treated as scrapy meta.

        :param article: The processed article item.
        :param to: The collection to save to.
        :return:
        """
        meta_dict = {}
        for key in list(article):
            if key[0].islower():
                meta_dict[key] = article[key]
                del article[key]
        article['_scrapy_meta'] = meta_dict
        article['last_updated'] = datetime.now()

        self.get_col(to).insert_one(article)

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

        return file_id

    def has_duplicate(self, where: Union[Collection, str], query, comparator: Optional[callable] = None) -> bool:
        """
        Check for duplicate items using a query.
        If any returned result matches the comparator, return True.

        :param where: The collection to check in.
        :param query: The mongo query to make.
        :param comparator: The comparator for documents.
        """
        col = self.get_col(where)

        results = col.find(query)
        if results.count() == 0:
            return False

        for i in results:
            if comparator:
                if comparator(i):
                    return True
            else:
                return True

    def get_col(self, name):
        if isinstance(name, Collection):
            return name
        return self.collections[name]

    def get_gridfs(self, name):
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

        for name, indices in self.collections_config:
            self.collections[name] = self.db[name]

            create_index(self.collections[name], indices)

        try:
            for name, indices in self.gridfs_config:
                self.gridfs[name] = gridfs.GridFS(self.db, collection=name)

                create_index(self.gridfs[name].__files, indices)
        except NotImplementedError:
            pass
