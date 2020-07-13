import urllib.parse
from pymongo import HASHED
from .ssrn_base import BaseSsrnSpider


class SsrnSpider_3526437(BaseSsrnSpider):
    name = 'ssrn_3526437'

    # DB specs
    collections_config = {
        'Scraper_papers_ssrn_com_3526437': [
            [('Doi', HASHED)],
            [('Title', HASHED)],
            'Publication_Date',
        ],
    }

    def build_query_url(self):
        query_dict = {
            'form_name': 'journalBrowse',
            'journal_id': '3526437',
        }

        return 'https://papers.ssrn.com/sol3/JELJOUR_Results.cfm?' + urllib.parse.urlencode(query_dict)
