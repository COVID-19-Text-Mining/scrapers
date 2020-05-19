import io
import re
from datetime import datetime
from urllib.parse import urljoin

import pandas
import scrapy
from pymongo import MongoClient, HASHED
from scrapy import Request


class ChictrSpider(scrapy.Spider):
    name = 'chictr'
    allowed_domains = ['www.chictr.org.cn']

    # DB specs
    db = None
    collection = None
    paper_fs = None
    collection_name = 'Scraper_chictr_org_cn'

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
        self.collection.create_index([('RegId', HASHED)])
        self.collection.create_index('CreatedTime')

    def insert_trial(self, trial):
        meta_dict = {}
        for key in list(trial):
            if key[0].islower():
                meta_dict[key] = trial[key]
                del trial[key]
        trial['_scrapy_meta'] = meta_dict
        trial['last_updated'] = datetime.now()

        self.collection.insert_one(trial)

    def start_requests(self):
        self.setup_db()

        yield Request(
            url='http://www.chictr.org.cn/filelisten.aspx',
            callback=self.find_covid_xlsx)

    def find_covid_xlsx(self, response):
        xlsx_fn = response.xpath(
            '//a[contains(text(), "Index of studies of Novel Coronavirus Pneumonia (COVID-19)")]/@href').extract_first()
        yield Request(
            url=urljoin(response.request.url, xlsx_fn),
            callback=self.parse_xlsx
        )

    def parse_xlsx(self, response):
        file = io.BytesIO(response.body)
        table = pandas.read_excel(file)

        for i, row in table.iterrows():
            yield Request(
                url=f'http://www.chictr.org.cn/historyversionpuben.aspx?regno={row["注册号"]}',
                callback=self.parse_history
            )

    def parse_history(self, response):
        table = response.xpath('//table[@class="table_list"]')
        rows = table.xpath('./tbody/tr')

        if len(rows) < 1:
            return

        for row in rows[1:]:
            version = float(row.xpath('./td[1]/text()').extract_first().strip())
            reg_id = row.xpath('./td[2]/text()').extract_first().strip()

            created_time = row.xpath('./td[3]/text()').extract_first().strip()
            created_time = datetime.strptime(created_time, '%Y/%m/%d %H:%M:%S')

            detail_url = row.xpath('./td[4]//a/@onclick').extract_first().strip()
            detail_url = re.search(r"window\.open\s*\(\s*[\"']([^\"']+)", detail_url).group(1)

            exist = False
            if self.collection.find_one({
                'RegId': reg_id,
                'CreatedTime': {'$gte': created_time}
            }) is not None:
                exist = True

            if not exist:
                yield Request(
                    url=urljoin(response.request.url, detail_url),
                    meta={
                        'RegId': reg_id,
                        'CreatedTime': created_time,
                        'Version': version,
                    },
                    callback=self.parse_detail
                )

            # only download the latest one
            break

    def parse_detail(self, response):
        def parse_row(cells):
            for i in range(0, len(cells), 2):
                title = cells[i].xpath('./p[@class="en"]/text()').extract_first()
                if title is None:
                    continue

                title = re.sub(r'[$\s.]+', ' ', title.strip('\r\n： '))

                value_cell = cells[i + 1]
                children_tbl = value_cell.xpath('./table')
                if len(children_tbl) > 0:
                    for child_tbl in children_tbl:
                        for k, v in parse_table(child_tbl):
                            yield f'{title}$${k}', v
                else:
                    val = cells[i + 1].xpath('./p[@class="en"]/text()').extract_first()
                    val = val or cells[i + 1].xpath('./text()').extract_first()
                    if val is not None:
                        yield title, val.strip()

        def parse_table(tbl):
            for body in tbl.xpath('./tbody'):
                cells = []
                for row in body.xpath('./tr'):
                    this_row = []
                    for cell in row.xpath('./td'):
                        this_row.append(cell)
                    cells.append(this_row)

                processed = set()
                # fix row span
                for i, row in enumerate(cells):
                    for j, cell in enumerate(row):
                        rowspan = cell.xpath('./@rowspan').extract_first() or '1'
                        rowspan = int(rowspan.strip())
                        if rowspan > 1 and cell not in processed:
                            for k in range(1, rowspan):
                                cells[i + k].insert(j, cell)
                            processed.add(cell)

                for row in cells:
                    yield from parse_row(row)

        info = {}
        for table in response.xpath('//div[@class="ProjetInfo_ms"]/table'):
            for key, value in parse_table(table):
                info[key] = value

        meta = response.meta
        meta['Data'] = info
        self.insert_trial(meta)
