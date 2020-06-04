# -*- coding: utf-8 -*-
import datetime
from urllib.parse import urlencode, urljoin

import scrapy
from pymongo import HASHED
from scrapy.utils.markup import remove_tags

from ._base import BaseSpider


class PatentSpider(BaseSpider):
    name = "lens_patent_spider"

    collections_config = {
        'Scraper_lens_patents': [
            [('Lens_ID', HASHED)],
            'Published_Date',
        ]
    }

    @staticmethod
    def build_lens_url(**kwargs):
        base_url = 'https://www.lens.org/lens/search/collection/179940'
        query_dict = {
            'dates': '+pub_date:19740101-20200402',  # dates
            'l': 'en',  # language
            'st': 'true',
            'n': 50,
            'p': 0,
            'v': 'table',
            's': 'pub_date',  # sort by
            'd': '+'
        }
        query_dict.update(kwargs)
        return '%s?%s' % (base_url, urlencode(query_dict))

    def start_requests(self):
        today = datetime.datetime.now().strftime("%Y%m%d")

        yield scrapy.Request(
            url=self.build_lens_url(
                dates=f"+pub_date:20200101-{today}"),
            callback=self.parse,
            dont_filter=True)

    def parse(self, response):
        patents_this_page = response.xpath('//*[contains(@class, "div-table-results-row")]')
        published_date = datetime.datetime(year=2000, month=1, day=1)

        for patent in patents_this_page:
            publication_number = ''.join(
                patent.xpath(
                    './/div[contains(@class, "doc-type")]//a/text()').extract()).strip()

            lens_id = ''.join(
                patent.xpath(
                    './/div[contains(@class, "lens-id")]//a/text()').extract()).strip()

            title = patent.xpath('.//h3//a/text()').extract_first().strip()

            published_data_raw = {}
            for entry in patent.xpath('.//ul[contains(@class, "header-meta")]/li'):
                key = entry.xpath('.//b/text()').extract_first().strip()
                key = key.strip(':')
                if not key:
                    continue

                value = ''.join(entry.xpath('./text()').extract()).strip()
                published_data_raw[key] = value

            try:
                published_date = datetime.datetime.strptime(
                    published_data_raw['Published'].replace(',', ''), '%b %d %Y')
            except (KeyError, ValueError):
                published_date = None

            try:
                filed_date = datetime.datetime.strptime(
                    published_data_raw['Filed'].replace(',', ''), '%b %d %Y')
            except (KeyError, ValueError):
                filed_date = None

            try:
                earliest_priority_date = datetime.datetime.strptime(
                    published_data_raw['Earliest Priority'].replace(',', ''), '%b %d %Y')
            except (KeyError, ValueError):
                earliest_priority_date = None

            applicants = published_data_raw.get('Applicant', [])
            if applicants:
                applicants = list(map(str.strip, applicants.split(',')))

            abstract_link = "https://www.lens.org/lens/patent/%s" % (lens_id,)

            if self.has_duplicate(
                    where='Scraper_lens_patents',
                    query={'Lens_ID': lens_id}):
                continue

            yield scrapy.Request(
                abstract_link,
                callback=self.parse_abstract,
                meta={
                    "Title": title,
                    "Publication_Number": publication_number,
                    "Lens_ID": lens_id,
                    "Link": abstract_link,
                    "Applicants": applicants,

                    "Published_Date": published_date,
                    "Filed_Date": filed_date,
                    "Earliest_Priority_Date": earliest_priority_date,
                },
                priority=10,
            )

        if len(patents_this_page) == 50:
            next_page_extend_link = response.xpath(
                './/a[contains(@class, "fa-chevron-right")]/@href')
            if len(next_page_extend_link) > 0:
                yield scrapy.Request(
                    urljoin(
                        response.request.url,
                        next_page_extend_link.extract_first().strip()),
                    callback=self.parse
                )
            else:
                old_date = published_date.strftime("%Y%m%d")
                today = datetime.datetime.now().strftime("%Y%m%d")
                new_link = self.build_lens_url(
                    dates="+pub_date:{old}-{today}".format(
                        old=old_date, today=today))
                yield scrapy.Request(
                    new_link,
                    callback=self.parse
                )

    def parse_abstract(self, response):
        meta = response.meta

        abstract_text = remove_tags(response.xpath(
            './/div[@class="page-title"]/following-sibling::p[1]').extract_first().strip()).strip()
        meta['Abstract'] = abstract_text

        links = response.xpath(".//a/@href").extract()
        html_link = None

        for link in links:
            if "fulltext" in link:
                html_link = "https://www.lens.org" + link
                break
        meta['HTML_Link'] = html_link

        if html_link is not None:
            yield scrapy.Request(
                html_link,
                callback=self.parse_full_text,
                meta=meta,
            )
        else:
            meta['Last_Updated'] = datetime.datetime.now()
            self.save_article(meta, to='Scraper_lens_patents')

    def parse_full_text(self, response):
        fulltext = response.xpath('.//div[@id="fullText"]').extract_first().strip()
        meta = response.meta
        meta['Full_Text'] = fulltext
        meta['Last_Updated'] = datetime.datetime.now()
        self.save_article(meta, to='Scraper_lens_patents')
