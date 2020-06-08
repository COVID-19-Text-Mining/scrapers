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
        def strip_extract(el, xpath):
            return list(map(str.strip, el.xpath(xpath).extract()))

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

                try:
                    paper_date, pubmed_id = strip_extract(row, './td[5]/font/text()')
                    paper_date = datetime.strptime(paper_date, '%m/%d/%Y')
                except ValueError:
                    # missing one element, try to guess which one it is.
                    try:
                        paper_date, = strip_extract(row, './td[5]/font/text()')
                        datetime.strptime('%m/%d/%Y', paper_date)
                        pubmed_id = '00000000'
                    except ValueError:
                        paper_date = None
                        pubmed_id, = strip_extract(row, './td[5]/font/text()')

                doi = row.xpath('./td[5]/font/span[@class="rNature"]/text()').extract_first()
                if doi is not None:
                    doi = doi.strip()

                retraction_date, retraction_pubmed_id = strip_extract(row, './td[6]/font/text()')
                retraction_doi = row.xpath('./td[6]/font/span[@class="rNature"]/text()').extract_first().strip()

                article_type = row.xpath('./td[7]/font/text()').extract_first().strip()
                nature = row.xpath('./td[7]/font/span[@class="rNature"]/text()').extract_first().strip()

                country = row.xpath('./td[8]/font/span[1]/text()').extract_first().strip()
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
                        'Date': datetime.strptime(retraction_date, '%m/%d/%Y'),
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

                if not self.has_duplicate(
                        where='Scraper_Retraction_database',
                        query={'Retraction_Id': retraction_id}):
                    self.save_article(article=data, to='Scraper_Retraction_database')
            except Exception as e:
                row_html = ''.join(row.extract()).replace("\n", " ")
                self.logger.exception(f'Failed to process row {e}: {row_html}')
