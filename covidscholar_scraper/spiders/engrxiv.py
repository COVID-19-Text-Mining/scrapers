import json
from json import JSONDecodeError

import scrapy
from pymongo import HASHED

from ._base import BaseSpider


class EngrxivSpider(BaseSpider):
    name = 'engrxiv'
    allowed_domains = ['osf.io']

    # DB specs
    collections_config = {
        'Scraper_engrxiv': [
            [('id', HASHED)],
            'last_updated'
        ]
    }

    url = 'https://share.osf.io/api/v2/search/creativeworks/_search?preference=ofqf4it64m'

    def make_request(self, start_from=0):
        data = {
            "query":
                {"bool": {
                    "must": {
                        "query_string": {"query": "COVID-19"}
                    },
                    "filter": [
                        {"bool": {
                            "should": [
                                {"terms": {"types": ["preprint"]}},
                                {"terms": {"sources": ["Thesis Commons"]}}
                            ]
                        }
                        },
                        {"terms": {
                            "sources": ["engrXiv"]
                        }
                        }
                    ]
                }
                },
            "from": start_from,
            "aggregations": {"sources": {"terms": {"field": "sources", "size": 500}}},
            "sort": {"date_updated": "desc"}
        }
        return scrapy.Request(
            self.url, method='POST',
            body=json.dumps(data),
            headers={'Content-Type': 'application/json'},
            meta={'from': start_from, 'dont_obey_robotstxt': True})

    def start_requests(self):
        yield self.make_request(0)

    def parse(self, response):
        try:
            data = json.loads(response.text)
        except JSONDecodeError:
            return

        has_new_element = False
        for item in data['hits']['hits']:
            item = item['_source']

            if self.has_duplicate(
                    where='Scraper_engrxiv',
                    query={'id': item['id']}):
                continue

            has_new_element = True

            self.save_article(item, to='Scraper_engrxiv', push_lowercase_to_meta=False)

        if has_new_element and response.meta['from'] + len(data['hits']['hits']) < data['hits']['total']:
            yield self.make_request(response.meta['from'] + len(data['hits']['hits']))
