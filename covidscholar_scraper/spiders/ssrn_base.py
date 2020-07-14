import re
import urllib.parse
from urllib.parse import urljoin
from pymongo import HASHED
from scrapy import Request
from datetime import datetime

from ._base import BaseSpider


class BaseSsrnSpider(BaseSpider):
    allowed_domains = ['papers.ssrn.com']
    name = 'ssrn'

    # DB specs
    collections_config = {
        'Scraper_papers_ssrn_com': [
            [('Doi', HASHED)],
            [('Title', HASHED)],
            'Publication_Date',
        ],
    }

    def start_requests(self):
        collections = ['3526432', '3526433', '3526437']

        for collection in collections:
            query_dict = {
                'form_name': 'journalBrowse',
                'journal_id': collection,
                'orderBy': 'ab_approval_date',
                'orderDir': 'desc',
            }
            url = 'https://papers.ssrn.com/sol3/JELJOUR_Results.cfm?' + urllib.parse.urlencode(query_dict)

            yield Request(
                url=url,
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                  '(KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36 Edg/83.0.478.58'},
                callback=self.parse_query_result,
                meta={'collection': collection}
            )

    def parse_query_result(self, response):
        papers = response.xpath(
            "//div[contains(@class, 'papers-list')]//div[contains(@class, 'description')]")
        has_new_papers = False
        for paper in papers:
            url = paper.xpath('.//a[contains(@class, "title")]/@href').extract_first()
            url = urljoin(response.request.url, url)

            paper_id = re.search(r'^https?://papers\.ssrn\.com/sol3/papers\.cfm\?abstract_id=(.*)$', url).group(1)
            if self.has_duplicate(
                    'Scraper_papers_ssrn_com',
                    {'Doi': "10.2139/ssrn.%s" % paper_id}):
                continue

            has_new_papers = True
            yield Request(
                url=url,
                priority=100,
                headers={'User-Agent':
                             'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36 Edg/83.0.478.58'},
                callback=self.parse_article,
                dont_filter=True,
                meta={'collection': response.meta['collection']}
            )

        next_page = response.xpath(
            "//div[@class='pagination']//li[@class='next']/a/@href").extract_first()
        if next_page is not None and has_new_papers:
            yield response.follow(
                next_page,
                callback=self.parse_query_result,
                meta={'collection': response.meta['collection']}
            )

    def parse_article(self, response):
        meta = {
            'JournalCollectionId': response.meta['collection'],
            'Journal': 'ssrn',
            'Origin': 'All preprints from srnn',
            'Title': response.xpath("//body//div[@class='box-container box-abstract-main']/h1/text()").get(),
            'Link': response.request.url
        }

        # Doi
        paper_id = re.search(r'^https://papers.ssrn.com/sol3/papers.cfm\?abstract_id=(.*)$', meta['Link']).group(1)
        meta['Doi'] = "10.2139/ssrn." + paper_id

        # Abstract
        meta['Abstract'] = (response.xpath(
            "string(//body//div[@class='box-container box-abstract-main']/div[@class='abstract-text']/p)").get()).strip()

        # Publication Date
        date = response.xpath(
            "//body//div[@class='box-container box-abstract-main']/p[@class='note note-list']/span/text()").extract()
        r_date = re.compile(r'^Posted:.*$')
        date = list(filter(r_date.match, date))[0]
        meta['Publication_Date'] = datetime.strptime(re.search(r'^Posted:\s(.*)$', date).group(1), '%d %b %Y')

        # Authors
        authors = response.xpath(
            "//div[@class='box-container box-abstract-main']/div[@class='authors authors-full-width']/h2/a/text()").extract() or response.xpath(
            "//div[@class='box-container box-abstract-main']/div[@class='authors cell authors-full-width']/h2/a/text()").extract()
        meta['Authors'] = [{'Name': x} for x in authors]

        if not self.has_duplicate(
                'Scraper_papers_ssrn_com',
                {'Doi': meta['Doi'],
                 'Publication_Date': meta['Publication_Date']}):
            self.save_article(meta, to='Scraper_papers_ssrn_com')
