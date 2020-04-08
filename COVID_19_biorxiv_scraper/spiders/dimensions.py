import pandas as pd
from pymongo import MongoClient, HASHED
import scrapy


class DimensionCOVIDScraper(scrapy.Spider):
    name = "dimensions"
    url = "https://www.dimensions.ai/news/dimensions-is-facilitating-access-to-covid-19-research/"

    sheet_names = ("Publications", "Clinical Trials", "Datasets")

    # change the collection names here
    collection_names = ["Dimensions_"+name.lower().replace(" ", "_") for name in sheet_names]

    # set entries
    entries = (
        [("publication_id", HASHED), ("doi", HASHED)],  # publications
        [("trial_id", HASHED)],  # clinical trials
        [("dataset_id", HASHED), ("doi", HASHED)]  # datasets
    )

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
            url=self.url,
            callback=self.get_download_page
        )

    def get_download_page(self, response):
        new_url = response.xpath(
            "//section[contains(@class, \"article-body py-6\")]/ul/li[2]/a/@href"
        ).extract_first()

        assert new_url, "Cannot find the url of the xlsx table"

        yield scrapy.Request(
            url=new_url,
            callback=self.download_table
        )

    def download_table(self, response):
        new_url = response.xpath(
            "//a[contains(@class, \"normal-link download-button shallow-button\")]/@href"
        ).extract_first()

        assert new_url, "Cannot find the url of the xlsx table"

        yield scrapy.Request(
            url=new_url,
            callback=self.parse
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
        sheet = pd.read_excel(table, sheet_name=sheet_name, parse_dates=[0], na_filter=False)
        indexes = [index.lower().replace(" ", "_") for index in sheet.columns]
        for row in sheet.values:
            assert len(row) == len(indexes), "wrong entry number"
            entry = dict(zip(indexes, row))
            self.insert_entry(keys, entry, collection)

    @staticmethod
    def insert_entry(keys, entry, collection):
        if collection.find_one({key: entry[key] for key in keys if entry[key]}) is None:
            collection.insert_one(entry)

    @staticmethod
    def date_parser(date: str):
        print(date)
        return pd.to_datetime(date, format("%Y-%m-%d")).date()
