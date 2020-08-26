import gzip
import io
import json
import re
import tarfile
import time
from datetime import datetime

import numpy as np
import pandas as pd
from pymongo import HASHED
from scrapy import Request

from ._base import BaseSpider


def correct_pd_dict(input_dict):
    """
        Correct the encoding of python dictionaries so they can be encoded to mongodb
        https://stackoverflow.com/questions/30098263/inserting-a-document-with-
        pymongo-invaliddocument-cannot-encode-object
        inputs
        -------
        input_dict : dictionary instance to add as document
        output
        -------
        output_dict : new dictionary with (hopefully) corrected encodings
    """

    output_dict = {}
    for key1, val1 in input_dict.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_pd_dict(val1)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)

        if isinstance(val1, set):
            val1 = list(val1)

        output_dict[key1] = val1

    return output_dict


class Cord19Spider(BaseSpider):
    name = 'cord_19'
    allowed_domains = ['semanticscholar.org']

    # DB specs
    collections_config = {
        'Scraper_Cord_19': [
            [('paper_id', HASHED)]
        ],
        'metadata': [
            [('data', HASHED)]
        ],
    }

    def start_requests(self):
        today = datetime.now()

        latest_scraped = self.get_col('metadata').find_one({'data': 'scrapers:cord_19_scraped'})
        if latest_scraped:
            next_date = datetime(
                year=latest_scraped['year'],
                month=latest_scraped['month'],
                day=latest_scraped['day'] + 1
            )
            if next_date > today:
                return
        else:
            next_date = datetime(year=today.year, month=today.month, day=today.day)

        data_file_path = next_date.strftime(
            'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/%Y-%m-%d/document_parses.tar.gz')

        self.get_col('metadata').update_one(
            {'data': 'scrapers:cord_19_scraped'},
            {'$set': {'year': next_date.year, 'month': next_date.month, 'day': next_date.day}},
            upsert=True
        )
        yield Request(
            url=data_file_path,
            callback=self.parse_gzip,
            meta={
                'download_maxsize': 0,
                'download_warnsize': 0,
            },
            dont_filter=True)

    def parse_gzip(self, response):
        fileio = io.BytesIO(response.body)
        gzipfile = gzip.GzipFile(fileobj=fileio, mode='rb')
        archive = tarfile.TarFile(fileobj=gzipfile, mode='r')

        for file in archive.getmembers():
            path = file.name

            m = re.search(r'.*?json$', path)
            if not m:
                continue

            contents = archive.extractfile(file)
            data = json.load(contents)
            paper_id = data['paper_id']

            def test_duplicate(old_doc):
                old_doc = {x: old_doc[x] for x in data}
                if old_doc == data:
                    return True

            if self.has_duplicate(
                    'Scraper_Cord_19',
                    {'paper_id': data['paper_id']},
                    comparator=test_duplicate):
                continue

            self.logger.info("Insert paper with id %s", paper_id)
            self.save_article(article=data, to='Scraper_Cord_19', push_lowercase_to_meta=False)
            # Sleep 3 secs to slow down insertion.
            time.sleep(3)

    def parse_csv(self, response):

        fileio = io.StringIO(response.body.decode('utf-8'))

        df = pd.read_csv(
            fileio,
            dtype={
                'pubmed_id': str,
                'pmcid': str,
                'publish_time': str,
                'Microsoft Academic Paper ID': str,
            }
        )
        df = df.fillna('')

        content_type = response.meta['content_type']
        collection = self.get_col(self.subset_collection_map[content_type])

        for i in range(len(df)):
            data = correct_pd_dict(df.iloc[i].to_dict())

            insert = True

            old_docs = collection.find({'cord_uid': data['cord_uid']}).sort('last_updated', -1)
            for old_doc in old_docs:
                old_doc = {x: old_doc.get(x, None) for x in data}
                if old_doc == data:
                    insert = False
                    break

            if insert:
                self.logger.info("Insert paper with cord_uid %s", data['cord_uid'])
                self.save_article(article=data, to=collection, push_lowercase_to_meta=False)
                # Sleep 3 secs to slow down insertion.
                time.sleep(3)
