import random
import time
from secrets import choice
from functools import wraps
import cloudscraper
import pandas as pd
import requests
from lxml import html
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import RequestException
from socket import timeout

from zentrada_eu.main import Functions, DataWriter


class Retry(object):
    def __init__(self, times, exceptions, pause=1, retreat=1,
                 max_pause=None, cleanup=None):
        self.times = times
        self.exceptions = exceptions
        self.pause = pause
        self.retreat = retreat
        self.max_pause = max_pause or (pause * retreat ** times)
        self.cleanup = cleanup

    def __call__(self, func):

        @wraps(func)
        def wrapped_func(*args):
            for i in range(self.times):
                pause = min(self.pause * self.retreat ** i, self.max_pause)
                try:
                    return func(*args)
                except self.exceptions:
                    if self.pause is not None:
                        print(f'problem nr {i} with {args}')
                        time.sleep(pause)

                    else:
                        pass
            if self.cleanup is not None:
                return self.cleanup(*args)

        return wrapped_func


class Products(Functions, DataWriter):
    def __init__(self):
        super(Products, self).__init__()
        # self.scrape_all_urls()
        # self.scrape_product()
        # self.scrape_product_1_by_1()
        self.scrape_all_prod_multithreading()

    def failed_call(*args, **kwargs):
        print("Failed call: " + str(args) + str(kwargs))

    retry = Retry(times=5, pause=1, retreat=2, cleanup=failed_call,
                  exceptions=(RequestException, timeout))

    @staticmethod
    def load_urls():
        with open(r'C:\Users\dklec\PycharmProjects\Zentrada\files\urls10.txt', 'r') as f:
            urls = f.readlines()
        urls = [x.replace('\n', '') for x in urls]
        return urls

    def scrape_all_prod_multithreading(self):
        urls = self.load_urls()

        threads, result = [], []

        with ThreadPoolExecutor(max_workers=50) as executor:
            [threads.append(executor.submit(self.scrape_product, url)) for url in urls]
            start = time.time()
            for i, task in enumerate(as_completed(threads)):
                print(f"{i} / {len(threads)}")
                task.result()

            end = time.time() - start
            print(end)
        self.main_output()

    @retry
    def scrape_product(self, url):
        data = {}
        s = cloudscraper.create_scraper()
        proxy = self.load_proxies()

        resp = s.get(url, proxies=proxy)
        self.extract_product_data(data, resp, url)

    @retry
    def scrape_product_1_by_1(self):
        data = {}
        urls = self.load_urls()
        s = cloudscraper.create_scraper()

        for i, url in enumerate(urls):
            proxy = self.load_proxies()
            resp = s.get(url, proxies=proxy)

            print(f"{i} / {len(urls)}")

            self.extract_product_data(data, resp, url)
        self.main_output()

    def extract_product_data(self, data, resp, url):

        while 'wiktorowski.dev@gmail.com' not in resp.text:
            print('Trying different proxy')
            s = cloudscraper.create_scraper()
            proxy = self.load_proxies()
            resp = s.get(url, proxies=proxy)

        category = "//div[@id='bredCrums']//text()"

        title = "//h1[@class='ym-mt5 ym-mb5']//text()"
        images = "//div[@class='innerBox']//div[contains(@class, 'detailImage')]//img/@src"

        packing_units = "//table[@class='shoppingCartTable']//td[@class='slidingB1']/text()"
        pieces = "//table[@class='shoppingCartTable']//td[@class='slidingB2']/text()"
        price_piece = "//table[@class='shoppingCartTable']//td[@class='slidingB3']/text()"
        net_value = "//table[@class='shoppingCartTable']//td[@class='slidingB4']/text()"

        additional_details = "//table[@class='shoppingCartTable']//td[@colspan]//text()"

        details_info_details_1 = "//div[@class='detailInfo ym-mt30']//text()"
        details_info_details_2 = "//div[@class='detailInfo ym-mb30']//text()"

        specification = "//p/../ul//li//text()"

        details_left = "//div[@class='detailLeft']//table[@id='articleInfo']//text()"
        details_right = "//div[@class='detailRight']//text()"

        category = self.extract(resp.text, category)
        title = self.extract(resp.text, title)
        images = html.fromstring(resp.text).xpath(images)
        packing_units = self.extract(resp.text, packing_units)
        pieces = self.extract(resp.text, pieces)
        price_piece = self.extract(resp.text, price_piece)
        net_value = self.extract(resp.text, net_value)
        additional_details = self.extract(resp.text, additional_details)
        details_info_details_1 = self.extract(resp.text, details_info_details_1)
        details_info_details_2 = self.extract(resp.text, details_info_details_2)
        specification = self.extract(resp.text, specification)
        details_left = self.extract(resp.text, details_left)
        details_right = self.extract(resp.text, details_right)

        properties = {
            'Link': url,
            'Category path': category,
            'Title': title,
            'Packing Units (PUs)': packing_units,
            'Pieces': pieces,
            'Price/Piece': price_piece,
            'Net Value': net_value,
            'Additional Details': additional_details,
            'Info Details 1': details_info_details_1,
            'Info Details 2': details_info_details_2,
            'Specification': specification,
            'Details Left': details_left,
            'Details Right': details_right,
        }

        for k, v in properties.items():
            data[k] = v

        for idx, x in enumerate(images):
            data[f'Image {idx + 1}'] = x

        self.products_output(data)

    @staticmethod
    def load_proxies():
        with open(r'C:\Users\dklec\PycharmProjects\Zentrada\zentrada_eu\proxies.txt') as f:
            proxies = f.read()
            proxies = proxies.split("\n")
            proxies = [x for x in proxies if x]
            proxies = [f'{p.split(":")[2]}:{p.split(":")[3]}@{p.split(":")[0]}:{p.split(":")[1]}' for p in proxies]
            proxies = [{'http': f'http://{i}', 'https': f'https://{i}'} for i in proxies]
            # proxies = [{'http': f'http://{i}'} for i in proxies]
        return choice(proxies)


if __name__ == '__main__':
    Products()
