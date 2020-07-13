import re
import urllib.parse

from pymongo import HASHED
from scrapy import Request
from datetime import datetime

from ._base import BaseSpider


class BaseSsrnSpider(BaseSpider):
    allowed_domains = ['papers.ssrn.com']

    def start_requests(self):
        yield Request(
            url=self.build_query_url(),
            headers={'User-Agent':
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36 Edg/83.0.478.58'},
            callback=self.parse_query_result,
        )

    def parse_query_result(self, response):
        ids = response.xpath("//div[@class='table results papers-list']/div[@class='tbody']/div/div[@class='description']/h3//a[@class='title optClickTitle']/@href").extract()
        for i in range(0, len(ids)):
            yield Request(
                url=ids[i],
                headers={'User-Agent':
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36 Edg/83.0.478.58'},
                callback=self.parse_article,
                dont_filter=True,
            )
        try:
            next_page = response.xpath("//body//div[@class='results-header']//div[@class='pagination']//li[@class='next']/a/@href").extract()[0]
            if next_page is not None:
                yield response.follow(next_page, callback=self.parse_query_result)
        except:
            pass

    def parse_article(self, response):
        meta = {}
        meta['Journal'] = 'ssrn'
        meta['Origin'] = 'All preprints from srnn'
        # Title
        meta['Title'] = response.xpath("//body//div[@class='box-container box-abstract-main']/h1/text()").get()

        # Link
        meta['Link'] = response.request.url

        # Doi
        paper_id = re.search(r'^https://papers.ssrn.com/sol3/papers.cfm\?abstract_id=(.*)$', meta['Link']).group(1)
        meta['Doi'] = "http://dx.doi.org/10.2139/ssrn." + paper_id

        # Abstract
        meta['Abstract'] = (response.xpath("string(//body//div[@class='box-container box-abstract-main']/div[@class='abstract-text']/p)").get()).strip()

        # Publication Date
        date = response.xpath("//body//div[@class='box-container box-abstract-main']/p[@class='note note-list']/span/text()").extract()
        r_date = re.compile(r'^Posted:.*$')
        date = list(filter(r_date.match, date))[0]
        meta['Publication_Date'] = datetime.strptime(re.search(r'^Posted:\s(.*)$', date).group(1), '%d %b %Y')

        # Authors
        authors = response.xpath("//div[@class='box-container box-abstract-main']/div[@class='authors authors-full-width']/h2/a/text()").extract() or response.xpath("//div[@class='box-container box-abstract-main']/div[@class='authors cell authors-full-width']/h2/a/text()").extract()
        meta['Authors'] = [{'Name': x} for x in authors]

        for name, _ in self.collections_config.items():
            self.save(name, meta)

    def save(self, name, meta):
        if not self.has_duplicate(
                    name,
                    {'Title': meta['Title'], 'Publication_Date': meta['Publication_Date']}):
                self.save_article(meta, to=name)
