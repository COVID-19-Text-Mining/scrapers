import gzip
import io
import json
import re
import tarfile
import time

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
        'CORD_comm_use_subset': [
            [('paper_id', HASHED)]
        ],
        'CORD_noncomm_use_subset': [
            [('paper_id', HASHED)]
        ],
        'CORD_biorxiv_medrxiv': [
            [('paper_id', HASHED)]
        ],
        'CORD_custom_license': [
            [('paper_id', HASHED)]
        ],
        'CORD_metadata': [
            [('cord_uid', HASHED)]
        ],
    }
    subset_collection_map = {
        'comm_use_subset': 'CORD_comm_use_subset',
        'noncomm_use_subset': 'CORD_noncomm_use_subset',
        'biorxiv_medrxiv': 'CORD_biorxiv_medrxiv',
        'custom_license': 'CORD_custom_license',
        'metadata': 'CORD_metadata',
    }

    def start_requests(self):
        data_files = [
            (
                'comm_use_subset',
                'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/comm_use_subset.tar.gz',
                self.parse_gzip,
            ), (
                'noncomm_use_subset',
                'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/noncomm_use_subset.tar.gz',
                self.parse_gzip,
            ), (
                'custom_license',
                'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/custom_license.tar.gz',
                self.parse_gzip,
            ), (
                'biorxiv_medrxiv',
                'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/biorxiv_medrxiv.tar.gz',
                self.parse_gzip,
            # ), (
            #     'metadata',
            #     'https://ai2-semanticscholar-cord-19.s3-us-west-2.amazonaws.com/latest/metadata.csv',
            #     self.parse_csv,
            ),
        ]
        for content_type, link, method in data_files:
            yield Request(
                url=link,
                callback=method,
                meta={
                    'download_maxsize': 0,
                    'download_warnsize': 0,
                    'content_type': content_type,
                },
                dont_filter=True)

    def parse_gzip(self, response):
        fileio = io.BytesIO(response.body)
        gzipfile = gzip.GzipFile(fileobj=fileio, mode='rb')
        archive = tarfile.TarFile(fileobj=gzipfile, mode='r')

        content_type = response.meta['content_type']
        collection = self.get_col(self.subset_collection_map[content_type])

        for file in archive.getmembers():
            path = file.name

            m = re.search(r'.*?json$', path)
            if not m:
                continue

            contents = archive.extractfile(file)
            data = json.load(contents)
            paper_id = data['paper_id']

            insert = True

            old_doc = collection.find_one({'paper_id': data['paper_id']})
            if old_doc is not None:
                old_doc = {x: old_doc[x] for x in data}
                if old_doc == data:
                    insert = False

            if insert:
                self.logger.info("Insert paper with id %s", paper_id)
                self.save_article(article=data, to=collection, push_lowercase_to_meta=False)
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
