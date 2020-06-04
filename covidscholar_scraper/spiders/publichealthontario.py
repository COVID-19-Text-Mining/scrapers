# -*- coding: utf-8 -*-
import re
from datetime import datetime

from bs4 import BeautifulSoup
from pymongo import HASHED
from scrapy import Request
from scrapy import Selector
from scrapy.http import TextResponse

from ._base import BaseSpider
from ..html_extractor.paragraphs import extract_paragraphs_recursive


class PublichealthontarioSpider(BaseSpider):
    name = 'publichealthontario'
    allowed_domains = ['publichealthontario.ca']

    # DB specs
    collections_config = {
        'Scraper_publichealthontario': [
            [('Link', HASHED)],
            'Date_Created',
        ]
    }
    gridfs_config = {
        'Scraper_publichealthontario_fs': []
    }

    pdf_parser_version = 'pho_20200423'
    pdf_laparams = {
        'char_margin': 1.0,
        'line_margin': 3.0
    }

    def start_requests(self):
        yield Request(
            url='https://www.publichealthontario.ca/en/diseases-and-conditions/'
                'infectious-diseases/respiratory-diseases/novel-coronavirus/articles',
            callback=self.parse)

    def save_object(self, result):
        result['PDF_gridfs_id'] = self.save_pdf(
            pdf_bytes=result['pdf_bytes'],
            pdf_fn=re.sub(r'[^a-zA-Z0-9]', '-', result['Title']) + '.pdf',
            pdf_link=result['Link'],
            fs='Scraper_publichealthontario_fs')
        del result['pdf_bytes']

        self.save_article(result, to='Scraper_publichealthontario')

    @staticmethod
    def find_abstract_by_parsing(content):
        # Parse the HTML
        paragraphs = extract_paragraphs_recursive(BeautifulSoup(content, features='html.parser'))

        def find_section(obj):
            if isinstance(obj, dict):
                if re.sub(r'[^\w]+', '', obj['name']).lower() == 'abstract':
                    return list(filter(lambda x: isinstance(x, str), obj['content']))
                elif isinstance(obj['content'], list):
                    for i in obj['content']:
                        r = find_section(i)
                        if r:
                            return r
            elif isinstance(obj, list):
                for i in obj:
                    r = find_section(i)
                    if r:
                        return r

            return []

        abstract = find_section(paragraphs)
        if not isinstance(abstract, list):
            abstract = [abstract]
        return abstract

    def handle_paper_page(self, response):
        meta = response.meta

        if isinstance(response, TextResponse):
            # Webpage parsing has a higher priority
            abstract = self.find_abstract_by_parsing(response.text)

            if len(abstract) == 0:
                # Try getting meta
                abstract = response.xpath('//meta[@name="citation_abstract"]/@content').extract()
                if len(abstract) == 0:
                    abstract = response.xpath('//meta[@name="dc.description"]/@content').extract()
                if len(abstract) == 0:
                    abstract = response.xpath('//meta[@name="description"]/@content').extract()
        else:
            abstract = None

        meta['Abstract'] = abstract
        self.save_object(meta)

    def handle_pdf(self, response):
        meta = response.meta
        meta['pdf_bytes'] = response.body
        meta['Synopsis_Link'] = response.request.url
        yield response.follow(
            url=meta['Link'],
            callback=self.handle_paper_page,
            dont_filter=True,
            meta=meta,
            priority=12,
        )

    def parse(self, response):
        for row in response.xpath('//table//tr').extract()[1:]:
            try:
                date_created = re.search(r'\w+\s+\d{1,2},\s+\d{4}', Selector(text=row).xpath(
                    '//td[1]').extract_first()).group(0)
            except AttributeError:
                continue
            authors = (Selector(text=row).xpath(
                '//td[2]/p/text()[1]').extract_first() or
                       Selector(text=row).xpath(
                           '//td[2]/text()[1]').extract_first())
            title = (Selector(text=row).xpath(
                '//td[2]/p/strong/text()').extract_first() or
                     Selector(text=row).xpath(
                         '//td[2]/strong/text()').extract_first())
            journal = (Selector(text=row).xpath(
                '//td[2]/p/text()[2]').extract_first() or
                       Selector(text=row).xpath(
                           '//td[2]/text()[2]').extract_first())
            link = (Selector(text=row).xpath(
                '//td[2]/p/a/@href').extract_first() or
                    Selector(text=row).xpath(
                        '//td[2]/a/@href').extract_first())
            desc = (Selector(text=row).xpath(
                '//td[3]/p/text()').extract_first() or
                    Selector(text=row).xpath(
                        '//td[3]/text()').extract_first())
            synopsis = (Selector(text=row).xpath(
                '//td[4]/p/a/@href').extract_first() or
                        Selector(text=row).xpath(
                            '//td[4]/a/@href').extract_first())

            if synopsis is None:
                continue

            old_items = self.get_col('Scraper_publichealthontario').find({'Link': link})
            insert = True
            for item in old_items:
                old_date = item['Date_Created'] or 'January 01, 1970'
                try:
                    old_date = datetime.strptime(old_date, '%B %d, %Y')
                    new_date = datetime.strptime(date_created, '%B %d, %Y')
                    if old_date >= new_date:
                        insert = False
                except ValueError:
                    continue

            if not insert:
                continue

            meta = {
                'Date_Created': date_created,
                'Authors': authors.strip().split(', '),
                'Title': re.sub(r'\s+', ' ', title),
                'Journal_String': re.sub(r'\s+', ' ', journal.strip()),
                'Link': link,
                'Desc': re.sub(r'\s+', ' ', desc),
            }
            yield response.follow(
                url=synopsis,
                callback=self.handle_pdf,
                dont_filter=True,
                meta=meta,
                priority=10,
            )
