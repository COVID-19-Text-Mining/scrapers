import io
import json
import re

from pymongo import HASHED
from scrapy import Request

from ._base import BaseSpider


class ThelancetSpider(BaseSpider):
    name = 'thelancet'

    # DB specs
    collections_config = {
        'Scraper_thelancet_com': [
            [('Doi', HASHED)],
            [('Title', HASHED)],
            'Publication_Date',
        ],
    }

    pdf_parser_version = 'thelancet_20200421'
    pdf_laparams = {
        'char_margin': 3.0,
        'line_margin': 2.5
    }

    url = 'https://www.thelancet.com/coronavirus/archive?startPage=0#navigation'

    def start_requests(self):
        yield Request(
            url=self.url,
            callback=self.parse,
            dont_filter=True,
            meta={ 'dont_obey_robotstxt': True },
        )

    def parse(self, response):
        meta = response.meta
        links = response.xpath('//body//div[@class="articleTitle"]//a/@href').extract()
        publish_dates = response.xpath('//body//div[@class="published-online"]/text()').extract()

        for i in range(0, len(publish_dates)):
            publish_dates[i] = (re.search(r'^Published:\s(.*)$', publish_dates[i])).group(1)

        titles = [h2.xpath('string(a)').get() for h2 in response.xpath('//body//div[@class="articleTitle"]/h2')]

        for article_number in range(0, len(links)):
            if not self.has_duplicate(
                    'Scraper_thelancet_com',
                    {'Title': titles[article_number], 'Publication_Date': publish_dates[article_number]}):
                meta['Title'] = titles[article_number]
                meta['Journal'] = 'thelancet'
                meta['Origin'] = 'All coronavirus articles from thelancet'
                meta['Publication_Date'] = publish_dates[article_number]
                meta['Link'] = 'https://www.thelancet.com' + links[article_number]
                yield Request(
                    url='https://www.thelancet.com' + links[article_number],
                    callback=self.parse_article,
                    meta=meta
                )
        next_page = response.xpath('//body//li[@class="next"]/a/@href').get()
        if next_page is not None:
            yield response.follow(next_page, callback=self.parse, meta={ 'dont_obey_robotstxt': True})

    def parse_article(self, response):
        meta = response.meta
        meta['Doi'] = response.xpath('//body//div[@class="inline-it"]//a/text()').extract_first()
        meta['Authors'] = [{'Name': x} for x in response.xpath('//body//li[@class="loa__item author"]/div[@class="dropBlock article-header__info"]/a/text()').extract()]
        meta['Text'] = self.find_text_html(response.text, meta['Title'])
        self.save_article(meta, to='Scraper_thelancet_com')
