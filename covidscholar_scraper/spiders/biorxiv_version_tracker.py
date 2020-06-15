from urllib.parse import urljoin

import sentry_sdk
from pymongo import HASHED
from scrapy import Request

from ._base import BaseSpider


class BiorxivVersionTrackerSpider(BaseSpider):
    name = 'biorxiv_version_tracker'
    allowed_domains = ['biorxiv.org', 'medrxiv.org']

    # DB specs
    collections_config = {
        'Scraper_connect_biorxiv_org': [],
        'Scraper_connect_biorxiv_org_new_versions': [
            [('Doi', HASHED)],
        ],
    }

    def start_requests(self):
        per_doi = {}
        for document in self.get_col('Scraper_connect_biorxiv_org').find():
            if self.has_duplicate(
                    'Scraper_connect_biorxiv_org_new_versions',
                    {'Doi': document['Doi']}):
                continue

            if document['Doi'] in per_doi:
                update_date, doc = per_doi[document['Doi']]
                if update_date >= document['last_updated']:
                    continue

            per_doi[document['Doi']] = (document['last_updated'], document)

        for doi, (_, document) in per_doi.items():
            yield Request(
                url=document['Link'] + '.article-info?versioned=true',
                callback=self.test_new_versions,
                meta=document)

    def test_new_versions(self, response):
        def url_to_doi(u):
            return '/'.join(u.split('?')[0].split('/')[-2:])

        version_urls = list(map(
            lambda x: urljoin(response.request.url, x),
            response.xpath('//div[contains(@class, "hw-versions")]//li/a/@href').extract()))
        versions = list(map(url_to_doi, version_urls))

        this_version = url_to_doi(response.meta['Link'])

        new_version_url = None

        for url, version in zip(version_urls, versions):
            if int(version.split('v')[-1]) > int(this_version.split('v')[-1]):
                new_version_url = url
                this_version = version

        if new_version_url is not None:
            site = 'biorxiv' if 'biorxiv.org' in new_version_url else 'medrxiv'
            self.logger.info('Registering new update job for DOI: %s Link: %s',
                             response.meta['Doi'], new_version_url)
            new_job = {
                'scrapy_url': new_version_url,
                'scrapy_site': site,
                'Doi': response.meta['Doi'],
                'Journal': site,
                'Publication_Date': response.meta['Publication_Date'],
                'Origin': response.meta['Origin']
            }

            with sentry_sdk.push_scope() as scope:
                scope.set_extra('DOI', response.meta['Doi'])
                sentry_sdk.capture_message('Scraper biorxiv: Updating article')
            sentry_sdk.flush()

            self.get_col('Scraper_connect_biorxiv_org_new_versions').insert_one(new_job)
