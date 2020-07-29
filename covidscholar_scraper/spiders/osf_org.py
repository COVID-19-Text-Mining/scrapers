import json
import re
from datetime import datetime

from pymongo import HASHED
from pytz import UTC
from scrapy.http import JsonRequest

from ._base import BaseSpider


def parse_date(s):
    try:
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%f%z')
    except ValueError:
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S%z')


class OsfOrgSpider(BaseSpider):
    name = 'osf_org'

    # DB specs
    collections_config = {
        'Scraper_osf_org': [
            [('doi', HASHED)],
            'date_updated',
        ],
    }
    gridfs_config = {
        'Scraper_osf_org_fs': [],
    }

    pdf_parser_version = 'preprints_org_20200727'
    pdf_laparams = {
        'char_margin': 3.0,
        'line_margin': 2.5
    }

    url = 'https://share.osf.io/api/v2/search/creativeworks/_search?preference=ex3807jvid'

    post_params = {
        "query": {
            "bool": {
                "must": {
                    "query_string": {"query": "*"}
                },
                "filter": [
                    {"bool": {
                        "should": [
                            {"terms": {"types": ["preprint"]}},
                            {"terms": {"sources": ["Thesis Commons"]}}]}
                    },
                    {"bool": {
                        "should": [
                            {"match": {"subjects": "bepress|Social and Behavioral Sciences"}},
                            {"match": {"subjects": "bepress|Law"}},
                            {"match": {"subjects": "bepress|Arts and Humanities"}}
                        ]}
                    },
                    {"terms": {
                        "sources": [
                            "OSF", "AfricArXiv", "AgriXiv", "Arabixiv", "BioHackrXiv", "BodoArXiv",
                            "EarthArXiv", "EcoEvoRxiv", "ECSarXiv", "EdArXiv", "engrXiv", "FocUS Archive",
                            "Frenxiv", "INA-Rxiv", "IndiaRxiv", "LawArXiv", "LIS Scholarship Archive", "MarXiv",
                            "MediArXiv", "MetaArXiv", "MindRxiv", "NutriXiv", "PaleorXiv", "PsyArXiv",
                            "Research AZ", "SocArXiv", "SportRxiv", "Thesis Commons", "arXiv", "bioRxiv",
                            "Preprints.org", "PeerJ", "Cogprints", "Research Papers in Economics"
                        ]}
                    }
                ]
            }
        },
        "from": 0,
        "aggregations": {
            "sources": {"terms": {"field": "sources", "size": 500}}
        },
        "sort": {"date_updated": "desc"}
    }

    def start_requests(self):
        params = self.post_params.copy()
        yield JsonRequest(
            url=self.url,
            data=params,
            callback=self.parse_results_list,
            meta={'dont_obey_robotstxt': True, 'from': 0}
        )

    def parse_results_list(self, response):
        data = json.loads(response.body)
        last_time = datetime.now().replace(tzinfo=UTC)
        has_new_paper = False

        for item in data['hits']['hits']:
            item = item['_source']
            item.update({
                'date': parse_date(item['date']),
                'date_created': parse_date(item['date_created']),
                'date_modified': parse_date(item['date_modified']),
                'date_published': parse_date(item['date_published']),
                'date_updated': parse_date(item['date_updated']),
            })
            last_time = min(last_time, item['date_updated'])

            try:
                doi = next(x for x in item['identifiers'] if re.match(r'^https?://(?:dx\.)?doi.org/.*$', x))
            except StopIteration:
                break

            item['doi'] = doi
            if self.has_duplicate(where='Scraper_preprints_org', query={'doi': doi}):
                continue

            has_new_paper = True
            self.save_article(item, to='Scraper_preprints_org', push_lowercase_to_meta=False)

        if has_new_paper and last_time > datetime(year=2020, month=1, day=1).replace(tzinfo=UTC):
            params = self.post_params.copy()
            params['from'] = response.meta['from'] + len(data['hits']['hits'])
            yield JsonRequest(
                url=self.url,
                data=params,
                callback=self.parse_results_list,
                meta={'dont_obey_robotstxt': True, 'from': params['from']}
            )
