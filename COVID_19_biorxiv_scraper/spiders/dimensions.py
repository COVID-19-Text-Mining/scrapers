import pandas as pd
from pymongo import MongoClient, HASHED
from numpy import nan
import scrapy


class DimensionCOVIDScraper(scrapy.Spider):
    name = "dimensions"
    url = "https://s3-eu-west-1.amazonaws.com/" \
          "pstorage-dimensions-5390582489/22163685/" \
          "DimensionsCOVID19publicationsdatasetsclinicaltrialsupdateddaily.xlsx"

    sheet_names = ("Publications", "Clinical Trials", "Datasets")

    # change the collection names here
    collection_names = [name.lower().replace(" ", "_") for name in sheet_names]

    # set entries
    entries = (
        [("publication_id", HASHED), ("doi", HASHED)],  # publications
        [("trial_id", HASHED)],  # clinical trials
        [("dataset_id", HASHED), ("doi", HASHED)]  # datasets
    )

    default_value = ""  # value for empty entries, bool(defalut_value) should return False

    query_keys = (
        ("publication_id", "doi"),
        ("trial_id",),
        ("dataset_id", "doi")
    )

    def setup_db(self):
        """
        Setup database and collection. Ensure indices.
        """
        self.db = MongoClient(
            host=self.settings['MONGO_HOSTNAME'],
        )[self.settings['MONGO_DB']]
        self.db.authenticate(
            name=self.settings['MONGO_USERNAME'],
            password=self.settings['MONGO_PASSWORD'],
            source=self.settings['MONGO_AUTHENTICATION_DB']
        )

    def start_requests(self):
        self.setup_db()

        yield scrapy.Request(
            url=self.url
        )

    def parse(self, response):
        table = response.body
        for sheet_name, collection_name, entries, keys in \
                zip(self.sheet_names, self.collection_names, self.entries, self.query_keys):
            collection = self.db[collection_name]
            for entry in entries:
                collection.create_index([entry])
            self.parse_sheet(table, sheet_name, collection, keys)

    def parse_sheet(self, table, sheet_name, collection, keys):
        sheet = pd.read_excel(table, sheet_name=sheet_name)
        indexes = [index.lower().replace(" ", "_") for index in sheet.columns]
        for row in sheet.values:
            row = [i if i is not nan else self.default_value for i in row]
            entry = dict(zip(indexes, row))
            self.insert_entry(keys, entry, collection)

    @staticmethod
    def insert_entry(keys, entry, collection):
        collection.replace_one(
            filter={key: entry[key] for key in keys if entry[key]},
            replacement=entry, upsert=True
        )
