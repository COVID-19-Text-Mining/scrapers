from collections import defaultdict
from datetime import datetime, timedelta

from pymongo import HASHED
from scrapy import Request, FormRequest

from ._base import BaseSpider


class RetractionDatabaseSpider(BaseSpider):
    name = 'retraction_database'
    allowed_domains = ['retractiondatabase.org']

    # DB specs
    collections_config = {
        'Scraper_Retraction_database': [
            [('Retraction_Id', HASHED)],
            'Retraction_Id',
        ],
    }

    def start_requests(self):
        yield Request(
            url='http://retractiondatabase.org/RetractionSearch.aspx',
            callback=self.make_request)

    def make_request(self, response):
        form_data = {}
        for i in response.xpath('//form//input'):
            name = i.xpath('./@name').extract_first()
            value = i.xpath('./@value').extract_first() or ''
            form_data[name] = value

        month_before = (datetime.now() - timedelta(days=30)).strftime('%m/%d/%Y')
        form_data['txtFromDate'] = month_before

        yield FormRequest(
            url=response.request.url,
            formdata=form_data,
            callback=self.parse_retraction_data
        )

    def parse_retraction_data(self, response):
        def strip_extract(el, *xpaths):
            c = []
            for xpath in xpaths:
                c = el.xpath(xpath).extract()
                if c:
                    break
            return list(map(str.strip, c))

        for row in response.xpath('//tr[@class="mainrow"]'):
            try:
                retraction_id = row.xpath('./td[1]/font/text()').extract_first().strip()

                paper_info = defaultdict(list)
                for span in row.xpath('./td[2]/font/a//span'):
                    name = span.xpath('./@class').extract_first()
                    value = span.xpath('./text()').extract_first()
                    if name and value and name.strip() and value.strip():
                        paper_info[name.strip().lstrip('r')].append(value.strip())
                for link in row.xpath('./td[2]/font/a/a/@href').extract():
                    paper_info['links'].append(link)
                paper_info = dict(paper_info)

                reasons = strip_extract(row, './td[3]/font/div[@class="rReason"]/text()')

                authors = strip_extract(row, './td[4]/font/a[@class="authorLink"]/text()')

                original_paper_info = strip_extract(row, './td[5]/font/text()', './td[5]/text()')
                if len(original_paper_info) == 2:
                    paper_date, pubmed_id = original_paper_info
                elif len(original_paper_info) == 1:
                    try:
                        paper_date, = original_paper_info
                        datetime.strptime('%m/%d/%Y', paper_date)
                        pubmed_id = '00000000'
                    except ValueError:
                        paper_date = None
                        pubmed_id, = original_paper_info
                else:
                    paper_date = None
                    pubmed_id = '00000000'
                if paper_date:
                    paper_date = datetime.strptime(paper_date, '%m/%d/%Y')

                doi = row.xpath('./td[5]/font/span[@class="rNature"]/text()').extract_first()
                if doi is not None:
                    doi = doi.strip()

                retraction_paper_info = strip_extract(row, './td[6]/font/text()')
                if len(retraction_paper_info) == 2:
                    retraction_date, retraction_pubmed_id = retraction_paper_info
                elif len(retraction_paper_info) == 1:
                    try:
                        retraction_date, = retraction_paper_info
                        datetime.strptime('%m/%d/%Y', retraction_date)
                        retraction_pubmed_id = '00000000'
                    except ValueError:
                        retraction_date = None
                        retraction_pubmed_id, = retraction_paper_info
                else:
                    retraction_date = None
                    retraction_pubmed_id = '00000000'
                if retraction_date:
                    retraction_date = datetime.strptime(retraction_date, '%m/%d/%Y')

                retraction_doi = row.xpath('./td[6]/font/span[@class="rNature"]/text()').extract_first()
                if retraction_doi is not None:
                    retraction_doi = retraction_doi.strip()

                article_type = row.xpath('./td[7]/font/text()').extract_first()
                if article_type:
                    article_type = article_type.strip()
                nature = row.xpath('./td[7]/font/span[@class="rNature"]/text()').extract_first()
                if nature:
                    nature = nature.strip()

                country = row.xpath('./td[8]/font/span[1]/text()').extract_first()
                if country:
                    country = country.strip()
                paywalled = row.xpath('./td[8]/font/span[1]/span[@class="rPaywalled"]/text()').extract_first()
                if paywalled is not None:
                    paywalled = paywalled.strip()

                notes = row.xpath('./td[8]/font/img[2]/@title').extract_first().strip()

                data = {
                    'Retraction_Id': retraction_id,
                    'Paper_Info': paper_info,
                    'Retraction_Reason': reasons,
                    'Authors': authors,
                    'Publication_Info': {
                        'Date': paper_date,
                        'PubMed_Id': pubmed_id,
                        'Doi': doi,
                    },
                    'Retraction_Info': {
                        'Date': retraction_date,
                        'PubMed_Id': retraction_pubmed_id,
                        'Doi': retraction_doi,
                    },
                    'Article_Type': article_type,
                    'Retraction_Nature': nature,
                    'Country': country,
                    'Paywalled': paywalled,
                    'Notes': notes,
                    'Source': 'http://retractiondatabase.org/'
                }
                data.update(response.meta)

                # print(data)
                # if not self.has_duplicate(
                #         where='Scraper_Retraction_database',
                #         query={'Retraction_Id': retraction_id}):
                #     self.save_article(article=data, to='Scraper_Retraction_database')
            except Exception as e:
                row_html = ''.join(row.extract()).replace("\n", " ")
                self.logger.exception(f'Failed to process row {e}: {row_html}')
