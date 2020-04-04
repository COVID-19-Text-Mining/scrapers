# -*- coding: utf-8 -*-
import calendar
import datetime
import gridfs
import scrapy
from scrapy.utils.markup import remove_tags
from pymongo import MongoClient, HASHED


class PatentSpider(scrapy.Spider):
    name = "patent_spider"
    json_file = "papers_info.json"

    def setup_db(self):
        """Setup database and collection. Ensure indices."""
        self.db = MongoClient(
            host=self.settings['MONGO_HOSTNAME'],
        )[self.settings['MONGO_DB']]
        self.db.authenticate(
            name=self.settings['MONGO_USERNAME'],
            password=self.settings['MONGO_PASSWORD'],
            source=self.settings['MONGO_AUTHENTICATION_DB']
        )
        self.collection = self.db[self.collection_name]

        # Create indices
        self.collection.create_index([('Publication Number', HASHED)])
        self.collection.create_index('published data')

        # Grid FS
        self.paper_fs = gridfs.GridFS(self.db, collection=self.collection_name + '_fs')

    def start_requests(self):
        self.setup_db()

        yield scrapy.Request(
            url="https://www.lens.org/lens/search/collection/179940?dates=%2Bpub_date:19740101-20200402&l=en&st=true&n=50&p=0&v=table&s=pub_date&d=%2B",
            callback=self.parse,
            dont_filter=True)

    def parse(self, response):
        patents_this_page = response.xpath('//*[contains(@class, "div-table-results-row")]')
        for patent in patents_this_page:
            publication_number = patent.xpath(".//a/@title").extract()[1].split(" - ")[0]
            exists = self.collection.find_one(
                {'Publication Number': publication_number}) is not None
            if exists:
                continue
            another_number = patent.xpath(".//a/@href")[-1].extract()
            title = patent.xpath(".//a/@title").extract()[1].split(" - ")[1]
            published_data_raw = patent.xpath('.//ul[@class="list-inline header-meta"]/li')[0].xpath(
                'string(.)').extract_first().strip().split("    ")[1]
            published_data_split = published_data_raw.split(", ")
            year = published_data_split[1]
            month_data = published_data_split[0].split(" ")
            month = "%02d" % list(calendar.month_abbr).index(month_data[0])
            date = "%02d" % int(month_data[1])
            published_date = str(year) + str(month) + str(date)
            abstract_link = "https://www.lens.org/lens/patent" + another_number
            yield scrapy.Request(
                abstract_link,
                callback=self.parse_abstract,
                meta={
                    "title": title,
                    "Publication Number": publication_number,
                    "another number": another_number,
                    "published data": published_date,
                    "abstract link": abstract_link
                },
            )
            # break
        if len(patents_this_page) == 50:
            next_page_extend_link = response.xpath(
                './/a[@class="settings-button fa fa-chevron-right"]/@href')
            if len(next_page_extend_link) > 0:
                next_page_link = "https://www.lens.org/" + next_page_extend_link.extract_first().strip()
                yield scrapy.Request(
                    next_page_link,
                    callback=self.parse
                )
            else:
                link_format = "https://www.lens.org/lens/search/collection/179940?p=9&st=true&s=pub_date&d=%2B&v=table&dates=%2Bpub_date:{old}-{today}&l=en&n=50"
                old_data_raw = patents_this_page[-1].xpath('.//ul[@class="list-inline header-meta"]/li')[0].xpath(
                    'string(.)').extract_first().strip().split("    ")[1]
                old_data_split = old_data_raw.split(", ")
                year = old_data_split[1]
                month_data = old_data_split[0].split(" ")
                month = "%02d" % list(calendar.month_abbr).index(month_data[0])
                date = "%02d" % int(month_data[1])
                old_date = str(year) + str(month) + str(date)
                today = "20" + datetime.date.today().strftime('%y%m%d')
                new_link = link_format.format(old=old_date, today=today)
                yield scrapy.Request(
                    new_link,
                    callback=self.parse
                )

    def parse_abstract(self, response):
        abstract_text = remove_tags(response.xpath(
            './/div[@class="page-title"]/following-sibling::p[1]').extract_first().strip()).strip()
        # print(abstract_text)

        links = response.xpath(".//a/@href").extract()
        html_link = ""

        for link in links:
            if "fulltext" in link:
                html_link = "https://www.lens.org" + link
                break
        if len(html_link) > 1:
            yield scrapy.Request(
                html_link,
                callback=self.parse_full_text,
                meta={
                    "title": response.meta["title"],
                    "Publication Number": response.meta["Publication Number"],
                    "another number": response.meta["another number"],
                    "published data": response.meta["published data"],
                    "abstract link": response.meta["abstract link"],
                    "abstract text": abstract_text,
                    "HTML link": html_link
                },
            )
        else:
            results = {
                "title": response.meta["title"],
                "Publication Number": response.meta["Publication Number"],
                "another number": response.meta["another number"],
                "published data": response.meta["published data"],
                "abstract link": response.meta["abstract link"],
                "abstract text": abstract_text
            }

            yield results

    def parse_full_text(self, response):

        results = {
            "title": response.meta['title'],
            "Publication Number": response.meta["Publication Number"],
            "another number": response.meta["another number"],
            "published data": response.meta["published data"],
            "abstract link": response.meta["abstract link"],
            "abstract text": response.meta["abstract text"],
            "HTML link": response.meta["HTML link"],
            "Full_text": response.xpath('.//div[@id="fullText"]').extract_first().strip()
        }

        yield results
