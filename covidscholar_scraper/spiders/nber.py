import re
from datetime import datetime

from pymongo import HASHED
from scrapy import Request

from ._base import BaseSpider


class NBERSpider(BaseSpider):
    allowed_domains = ['nber.org']
    name = 'nber'

    # DB specs
    collections_config = {
        'Scraper_nber_org': [
            [('Doi', HASHED)],
            [('NBER_Article_Number', HASHED)],
            'Publication_Date',
        ],
    }
    gridfs_config = {
        'Scraper_nber_org_fs': [],
    }

    pdf_parser_version = 'nber_20200715'
    pdf_laparams = {
        'char_margin': 3.0,
        'line_margin': 2.5
    }

    def start_requests(self):
        yield Request(
            url='https://www.nber.org/new.html#latest',
            callback=self.parse_all_links,
        )
        # yield Request(
        #     url='https://www.nber.org/new_archive/2020.html',
        #     callback=self.parse_all_links,
        # )

    def parse_all_links(self, response):
        for url in response.xpath('//a/@href').extract():
            m = re.match(r'^https?://www\.nber\.org/papers/([a-zA-Z0-9]+)$', url)
            if not m:
                continue

            article_number = m.group(1)
            if not self.has_duplicate(
                    'Scraper_nber_org',
                    {'NBER_Article_Number': article_number}):
                yield Request(
                    url=url,
                    callback=self.parse_page,
                )

    def parse_page(self, response):
        title = response.xpath(
            '//h1[contains(@class, "title")]/text()').extract_first().strip()
        authors = list(map(
            str.strip,
            response.xpath('//h2[contains(@class, "citation_author")]//a/text()').extract()))
        abstract = response.xpath(
            '//p[@style="margin-left: 40px; margin-right: 40px; text-align: justify"]/text()').extract_first().strip()
        article_number = response.xpath(
            '//meta[@name="citation_technical_report_number"]/@content').extract_first().strip()
        pub_date = response.xpath(
            '//meta[@name="citation_publication_date"]/@content').extract_first().strip()
        pub_date = datetime.strptime(pub_date, '%Y/%m/%d')

        doi = f'10.3386/{article_number}'

        data = {
            'NBER_Article_Number': article_number,
            'Doi': doi,
            'Link': response.request.url,
            'Authors': authors,
            'Title': title,
            'Abstract': abstract,
            'Publication_Date': pub_date,
        }

        if not self.has_duplicate(
                'Scraper_nber_org',
                {'NBER_Article_Number': article_number}):
            yield Request(
                url=f'https://www.nber.org/papers/{article_number}.pdf',
                priority=100,
                callback=self.handle_pdf,
                meta={'Data': data}
            )

    def handle_pdf(self, response):
        data = response.meta['Data']

        pdf_id = self.save_pdf(
            response.body,
            pdf_fn=f'NBER-{data["NBER_Article_Number"]}.pdf',
            pdf_link=response.request.url,
            fs='Scraper_nber_org_fs')

        data['PDF_gridfs_id'] = pdf_id
        self.save_article(data, to='Scraper_nber_org')
