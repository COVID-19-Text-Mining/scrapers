import io
import re
from datetime import datetime
from urllib.parse import urljoin

import pandas
from pymongo import HASHED
from scrapy import Request

from ._base import BaseSpider


class ChictrSpider(BaseSpider):
    name = 'chictr'
    allowed_domains = ['www.chictr.org.cn']

    # DB specs
    collections_config = {
        'Scraper_chictr_org_cn': [
            [('RegId', HASHED)],
            'CreatedTime'
        ]
    }

    def start_requests(self):
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

            if not self.has_duplicate(
                    where='Scraper_chictr_org_cn',
                    query={
                        'RegId': reg_id,
                        'CreatedTime': {'$gte': created_time}
                    }):
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

        self.save_article(meta, to='Scraper_chictr_org_cn')
