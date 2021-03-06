import json
import logging
import re
from datetime import datetime

from pymongo import HASHED
from scrapy import Request, Selector

from ._base import BaseSpider


class BiorxivSpider(BaseSpider):
    name = 'biorxiv'
    allowed_domains = ['biorxiv.org', 'medrxiv.org']
    json_source = 'https://connect.biorxiv.org/relate/collection_json.php?grp=181'
    html_source = 'http://connect.biorxiv.org/relate/content/181?page={page}'

    # DB specs
    collections_config = {
        'Scraper_connect_biorxiv_org': [
            [('Doi', HASHED)],
            'Publication_Date',
        ],
        'Scraper_connect_biorxiv_org_new_versions': [],
    }
    gridfs_config = {
        'Scraper_connect_biorxiv_org_fs': [],
    }

    pdf_parser_version = 'biorxiv_20200421'
    pdf_laparams = {
        'char_margin': 3.0,
        'line_margin': 2.5
    }

    def updated_articles(self, site_table):
        tracking_col = self.get_col('Scraper_connect_biorxiv_org_new_versions')
        updates_to_remove = []
        for update in tracking_col.find():
            yield Request(
                url=update['scrapy_url'],
                callback=site_table[update['scrapy_site']],
                meta={
                    'Doi': update['Doi'],
                    'Journal': update['Journal'],
                    'Publication_Date': update['Publication_Date'],
                    'Origin': update['Origin'],
                })
            updates_to_remove.append(update['_id'])

        for i in range(0, len(updates_to_remove), 100):
            end = min(len(updates_to_remove), 100 + i)
            tracking_col.delete_many({'_id': {'$in': updates_to_remove[i:end]}})

    def start_requests(self):
        yield from self.updated_articles(site_table={
            'medrxiv': self.handle_medrxiv,
            'biorxiv': self.handle_biorxiv,
        })

        yield Request(
            url=self.html_source.format(page=1),
            callback=self.handle_article_list,
            meta={'page': 1})

    def handle_article_list(self, response):
        site_table = {
            'medrxiv': self.handle_medrxiv,
            'biorxiv': self.handle_biorxiv,
        }

        has_paper = False
        for entry in response.xpath('//div[contains(@class, "highwire-article-citation")]'):
            link = entry.xpath('.//a[contains(@class, "highwire-cite-linked-title")]/@href').extract_first().strip()
            doi = entry.xpath('.//a[contains(@class, "highwire-cite-metadata")]//a/text()').extract_first()
            if doi is None:
                doi = '10.1101/' + link.rsplit('/')[-1]
            doi = doi.strip()

            if 'biorxiv' in link:
                site = 'biorxiv'
            elif 'medrxiv' in link:
                site = 'medrxiv'
            else:
                continue

            if self.has_duplicate(
                    'Scraper_connect_biorxiv_org',
                    {'Doi': doi}):
                continue

            has_paper = True
            yield Request(
                url=link,
                dont_filter=True,
                callback=site_table[site],
                # Pass in initial meta data dictionary
                meta={
                    'Doi': doi,
                    'Journal': site,
                    'Origin': 'COVID-19 SARS-CoV-2 preprints from medRxiv and bioRxiv @ %s' % self.json_source,
                })

        if has_paper:
            yield Request(
                url=self.html_source.format(page=response.meta['page'] + 1),
                callback=self.handle_article_list,
                meta={'page': response.meta['page'] + 1}
            )

    def handle_medrxiv_pdf(self, response):
        result = response.meta

        file_id = self.save_pdf(
            pdf_bytes=response.body,
            pdf_fn=result['Doi'].replace('/', '-') + '.pdf',
            pdf_link=result['Link'],
            fs='Scraper_connect_biorxiv_org_fs')

        result['PDF_gridfs_id'] = file_id

        self.save_article(result, 'Scraper_connect_biorxiv_org')

    def handle_medrxiv(self, response):
        result = response.meta
        result['Link'] = response.request.url

        # Scrape title
        result['Title'] = self.get_all_text_html(response.xpath("//*[contains(@id,'page-title')]").extract_first())

        result['Publication_Date'] = datetime.strptime(
            response.xpath('//meta[@name="DC.Date"]/@content').extract_first(), '%Y-%m-%d')

        # Scrape author list
        authors = []
        for name, content in zip(
                response.xpath("//meta[contains(@name,'citation_author')]/@name").extract(),
                response.xpath("//meta[contains(@name,'citation_author')]/@content").extract()):
            if name == 'citation_author':
                # Split FN/LN by whitespace
                names = content.strip().rsplit(' ', maxsplit=1)
                if len(names) == 2:
                    fn, ln = names
                else:
                    fn, ln = '', names[0]
                authors.append({
                    'Name': {'fn': fn, 'ln': ln}
                })
            else:
                # Some values can be a list
                key = name[len('citation_author'):].strip('_').capitalize()
                if key not in authors[-1]:
                    authors[-1][key] = []
                authors[-1][key].append(content)
        result['Authors'] = authors

        # Abstract
        result['Abstract'] = []
        for p in response.xpath("//div[contains(@class, 'abstract') and contains(@class, 'section')]//p").extract():
            p = re.sub(r'\s+', ' ', ' '.join(Selector(text=p).xpath('//text()').extract()))
            result['Abstract'].append(p)

        # Subject area
        result['Subject_Area'] = list(filter(
            lambda x: len(x),
            map(
                lambda x: x.strip(),
                response.xpath("//div[contains(@class, 'pane-highwire-article-collections')]//li//text()").extract()
            )
        ))

        yield Request(
            url=response.request.url + '.full.pdf',
            meta=result,
            callback=self.handle_medrxiv_pdf,
            priority=10,
        )

    def handle_biorxiv(self, response):
        # They share the same page structure
        return self.handle_medrxiv(response)
