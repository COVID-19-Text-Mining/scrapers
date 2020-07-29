import re
from datetime import datetime
from urllib.parse import urljoin

from pymongo import HASHED
from scrapy import Request

from ._base import BaseSpider


class PrePrintsOrgSpider(BaseSpider):
    name = 'preprints_org'

    # DB specs
    collections_config = {
        'Scraper_preprints_org': [
            [('Doi', HASHED)],
            'Publication_Date',
        ],
    }
    gridfs_config = {
        'Scraper_preprints_org_fs': [],
    }

    pdf_parser_version = 'preprints_org_20200727'
    pdf_laparams = {
        'char_margin': 3.0,
        'line_margin': 2.5
    }

    url = 'https://www.preprints.org/covid19?order_by=most_recent&page_num={page_num}'

    def build_url(self, page):
        return self.url.format(page_num=page)

    def start_requests(self):
        yield Request(
            url=self.build_url(1),
            meta={'page': 1},
            callback=self.parse_results
        )

    def parse_results(self, response):
        last_time = datetime.now()
        has_new_paper = False

        for item in response.xpath('//div[contains(@class, "search-wrapper")]/div'):
            doi_parts = item.xpath(
                './/a[@class="title"]/@href').extract_first().strip().split('/')[-2:]
            doi = f'10.20944/preprints{".".join(doi_parts)}'

            article_type = item.xpath(
                './/span[contains(@class, "content-box-header-element-1")]/text()').extract_first().capitalize()

            title = item.xpath('.//a[@class="title"]').extract_first()
            title = self.get_all_text_html(title)

            authors = item.xpath('.//a[contains(@class, "author-selector")]/text()').extract()
            subjects = item.xpath('.//a[contains(@href, "search_subject")]//text()').extract()
            keywords = item.xpath('.//a[contains(@href, "=keywords")]//text()').extract()

            publication_date = item.xpath(
                './/div[4]//span[contains(@class, "search-content-header-label")]/text()').extract_first()
            publication_date = re.search(r'Online: (\d+\s+\w+\s+\d+)', publication_date).group(1)
            publication_date = datetime.strptime(publication_date, '%d %B %Y')

            abstract = item.xpath('.//div[contains(@class, "abstract-content")]').extract_first()
            abstract_text = self.get_all_text_html(abstract)

            pdf_link = item.xpath('.//a[contains(@href, "/download")]/@href').extract_first()
            pdf_link = urljoin(response.request.url, pdf_link)

            item = {
                'Doi': doi,
                'ArticleType': article_type,
                'Title': title,
                'Authors': authors,
                'Subjects': subjects,
                'Keywords': keywords,
                'Publication_Date': publication_date,
                'Abstract': abstract_text,
                'PDF_Link': pdf_link,
            }
            last_time = min(last_time, publication_date)

            if self.has_duplicate(where='Scraper_preprints_org', query={'Doi': doi}):
                continue

            has_new_paper = True
            yield Request(
                url=pdf_link,
                callback=self.handle_pdf,
                meta=item
            )

        if has_new_paper and last_time > datetime(year=2020, month=1, day=1):
            page = response.meta['page'] + 1
            yield Request(
                url=self.build_url(page),
                meta={'page': page},
                callback=self.parse_results
            )

    def handle_pdf(self, response):
        result = response.meta

        file_id = self.save_pdf(
            pdf_bytes=response.body,
            pdf_fn=result['Doi'].replace('/', '-') + '.pdf',
            pdf_link=result['PDF_Link'],
            fs='Scraper_preprints_org_fs')

        result['PDF_gridfs_id'] = file_id

        self.save_article(result, 'Scraper_preprints_org')
