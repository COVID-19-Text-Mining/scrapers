import json
from datetime import datetime
from json import JSONDecodeError

from pymongo import HASHED
from scrapy import Request

from ._base import BaseSpider


class ZenodoSpider(BaseSpider):
    name = 'zenodo'
    allowed_domains = ['zenodo.org']

    # DB specs
    collections_config = {
        'Scraper_zenodo_org': [
            [('doi', HASHED)],
            'created',
            'last_updated'
        ]
    }

    start_urls = ['https://zenodo.org/api/records/?page=1&size=20&communities=covid-19']

    def parse(self, response):
        try:
            data = json.loads(response.text)
        except JSONDecodeError:
            return

        has_new_element = False
        for item in data['hits']['hits']:
            if self.has_duplicate(
                    where='Scraper_zenodo_org',
                    query={'doi': item['doi']}):
                continue

            has_new_element = True
            item['last_updated'] = datetime.now()
            self.get_col('Scraper_zenodo_org').insert(item)

        if has_new_element and 'next' in data['links']:
            yield Request(
                url=data['links']['next']
            )
