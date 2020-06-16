import re
import time

from datetime import datetime
import scrapy
from pymongo import HASHED
from scrapy import Request
import urllib.parse

from ._base import BaseSpider


class ArxivSpider(BaseSpider):
    name = 'arxiv'

    # DB specs
    collections_config = {
        'Scraper_arxiv_org': [
            [('Arxiv_id', HASHED)],
            [('Title', HASHED)],
            'Publication_Date',
        ],
    }

    sleep_time = 5

    def build_query_url(self):
        query_dict = {
            "advanced":"",
            "terms-0-operator":"AND",
            "terms-0-term":"COVID-19",
            "terms-0-field":"title",
            "terms-1-operator":"OR",
            "terms-1-term":"SARS-CoV-2",
            "terms-1-field":"abstract",
            "terms-3-operator":"OR",
            "terms-3-term":"COVID-19",
            "terms-3-field":"abstract",
            "terms-4-operator":"OR",
            "terms-4-term":"SARS-CoV-2",
            "terms-4-field":"title",
            "terms-5-operator":"OR",
            "terms-5-term":"coronavirus",
            "terms-5-field":"title",
            "terms-6-operator":"OR",
            "terms-6-term":"coronavirus",
            "terms-6-field":"abstract",
            "classification-physics_archives":"all",
            "classification-include_cross_list":"include",
            "date-filter_by":"all_dates",
            "date-year":"",
            "date-from_date":"",
            "date-to_date":"",
            "date-date_type":"submitted_date",
            "abstracts":"show",
            "size":"200",
            "order":"-announced_date_first",
            "source":"home-covid-19"
        }
        return 'https://arxiv.org/search/advanced?' + urllib.parse.urlencode(query_dict)

    def start_requests(self):
        yield Request(
            url=self.build_query_url(),
            callback=self.parse_query_result,
            meta={ 'dont_obey_robotstxt': True }
        )

    def parse_query_result(self, response):
        # titles
        titles = [p.xpath('string(p[@class="title is-5 mathjax"])').get() for p in response.xpath('//body//ol/li[@class="arxiv-result"]')]
        titles = [re.search(r'([^\n\s].*)\n', title).group(1) for title in titles]

        # dates
        # Choosing the date of submission of latest version as the publication date
        publication_dates = [h2.xpath('string(p[@class="is-size-7"])').get() for h2 in response.xpath('//body//ol/li[@class="arxiv-result"]')]
        publication_dates = [datetime.strptime(re.search(r'^Submitted\s(.*?);', publication_date).group(1), '%d %B, %Y') for publication_date in publication_dates]

        # authors
        authors = []
        for a in response.xpath('//body//ol[@class="breathe-horizontal"]/li[@class="arxiv-result"]/p[@class="authors"]'):
            authors.append([{'Name': x} for x in a.xpath('a/text()').getall()])

        # arxiv ids
        arxiv_ids = response.xpath('//body//ol[@class="breathe-horizontal"]/li[@class="arxiv-result"]//p[@class="list-title is-inline-block"]/a/text()').getall()
        arxiv_ids = [re.search(r'^arXiv:(.*)$', arxiv_id).group(1) for arxiv_id in arxiv_ids]

        # abstracts
        abstracts = [h.xpath('string(span[@class="abstract-full has-text-grey-dark mathjax"])').get() for h in response.xpath('//body//ol[@class="breathe-horizontal"]/li[@class="arxiv-result"]/p[@class="abstract mathjax"]')]
        abstracts = [re.search(r'([^\n\s].*)\n', abstract).group(1) for abstract in abstracts]

        for paper_num in range(0, len(titles)):
            if not self.has_duplicate(
                    'Scraper_arxiv_org',
                    {'Arxiv_id': arxiv_ids[paper_num]}):
                meta = {}
                meta['Title'] = titles[paper_num]
                meta['Journal'] = 'arxiv'
                meta['Origin'] = 'All covid-19 preprints from arxiv'
                meta['Publication_Date'] = publication_dates[paper_num]
                meta['Authors'] = authors[paper_num]
                meta['Link'] = "https://arxiv.org/abs/" + arxiv_ids[paper_num]
                meta['Arxiv_id'] = arxiv_ids[paper_num]
                meta['Abstract'] = abstracts[paper_num]
                self.save_article(meta, to='Scraper_arxiv_org')
        print("sleeping for {} seconds".format(self.sleep_time))
        time.sleep(self.sleep_time)
        try:
            next_page = response.xpath('//body//nav[@class="pagination is-small is-centered breathe-horizontal"]/a[@class="pagination-next"]/@href').extract()[0]
            if next_page is not None:
                yield response.follow(next_page, callback=self.parse_query_result, meta=response.meta)
        except:
            pass
