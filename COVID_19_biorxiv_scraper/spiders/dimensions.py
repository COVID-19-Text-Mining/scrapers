import requests
import logging
import pandas as pd
from pymongo import MongoClient, HASHED
import gridfs
from numpy import nan

logger = logging.getLogger(__name__)


class DimensionCOVIDScraper:
    user_agent = "COVID-19 Scholar: Text-mining for COVID-19 research @ LBNL " \
                 "(+http://covidscholar.com/) (+covid19textmining@googlegroups.com)"

    url = "https://s3-eu-west-1.amazonaws.com/" \
          "pstorage-dimensions-5390582489/22163685/" \
          "DimensionsCOVID19publicationsdatasetsclinicaltrialsupdateddaily.xlsx"

    settings = {}
    collection_name = ""
    sheet_names = ("Publications", "Clinical Trials", "Datasets")

    default_value = ""

    def __init__(self):
        """
        Setup database and collection.
        """
        self.db = MongoClient(
            host=self.settings['MONGO_HOSTNAME'],
        )[self.settings['MONGO_DB']]
        self.db.authenticate(
            name=self.settings['MONGO_USERNAME'],
            password=self.settings['MONGO_PASSWORD'],
            source=self.settings['MONGO_AUTHENTICATION_DB']
        )
        self.collection = self.db[self.collection_name]

    def pipeline(self):
        logger.info("Dimensions scraper starts.")
        table = self.download_file()
        for sheet_name in self.sheet_names:
            self.parse_sheet(table, sheet_name)
        logger.info("Dimensions scraper ends. (Finished)")

    def download_file(self):
        with requests.Session() as req:
            self.user_agent and req.headers.update({"User-Agent": self.user_agent})
            response = req.get(self.url)
            response.raise_for_status()
        logger.info("Download the file successfully.")
        return response.content

    def parse_sheet(self, table, sheet_name):
        sheet = pd.read_excel(table, sheet_name=sheet_name)
        sheet_name = sheet_name.lower().replace(" ", "_")
        indexes = [index.lower().replace(" ", "_") for index in sheet.columns]
        for row in sheet.values:
            row = [i if i is not nan else self.default_value for i in row]
            entry = dict(zip(indexes, row))
            entry["source_type"] = sheet_name
            self.insert_entry(entry)

    def insert_entry(self, entry):
        if self.collection.find_one(entry) is None:
            logger.info("Get new paper: {}".format(entry))
            self.collection.update_one(entry, upsert=True)


if __name__ == "__main__":
    DimensionCOVIDScraper().pipeline()
