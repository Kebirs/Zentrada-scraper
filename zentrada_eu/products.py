import time
from secrets import choice

import cloudscraper
import pandas as pd
import requests
from lxml import html
from concurrent.futures import ThreadPoolExecutor, as_completed

from requests.exceptions import ProxyError

from zentrada_eu.main import Functions, DataWriter


class Products(Functions, DataWriter):
    def __init__(self):
        super(Products, self).__init__()
        self.proxy_status = ''
        self.status = ''
        # self.scrape_all_urls()
        # self.init_links()
        # self.scrape_all_products()
        # self.scrape_product()
        self.scrape_all_prod_multithreading()

    def init_links(self):

        all_urls = []
        auth = {'CFID': '9e159929-0b55-4a6b-96e0-ff64e2e4623a',
                'CFTOKEN': '0',
                'PageNum': '1'}
        url = 'https://www.zentrada.eu/'
        resp = self.get_response(url, auth, '')

        root_urls = "//div[@id='productgroupLeft']//a/@href"
        root_urls = html.fromstring(resp.text).xpath(root_urls)
        for i, url in enumerate(root_urls):
            print(f'root {i + 1} / {len(root_urls)}')
            static_max_page = 1000
            for page in range(1, static_max_page):
                auth['PageNum'] = page
                auth['CFTOKEN'] = '0,0'
                auth['CFID'] = ''
                resp_3 = self.get_response(url, auth, '')

                prods = "//div[@id='productlistZentrada2']//a[contains(@href, 'product.cfm')]/@href"
                prods = html.fromstring(resp_3.text).xpath(prods)
                if len(prods) > 0:
                    print(f'Page {page}')
                    for prod in prods:
                        all_urls.append(prod)
                        if "9e159929-0b55-4a6b-96e0-ff64e2e4623a" not in prod:
                            print(prod)
                else:
                    break
            df = pd.DataFrame(all_urls)
            df.to_csv('zentrada_urls.csv', index=False)

        df = pd.DataFrame(all_urls)
        df.to_csv('zentrada_urls.csv', index=False)
        return all_urls

    def scrape_all_urls(self):
        # 9e159929-0b55-4a6b-96e0-ff64e2e4623a
        # c97e38a8-6e5d-438e-97f2-3e295ca8ea03

        # 342ac053-ec02-4b8e-b2a7-8fafd9c4fa2a
        auth = {'CFID': '342ac053-ec02-4b8e-b2a7-8fafd9c4fa2a',
                'CFTOKEN': '0',
                'PageNum': '1'}
        url = 'https://www.zentrada.eu/'
        resp = self.get_response(url, auth, self.load_proxies())

        root_urls = "//div[@id='productgroupLeft']//a/@href"
        root_urls = html.fromstring(resp.text).xpath(root_urls)

        # threads, result = [], []
        #
        # with ThreadPoolExecutor(max_workers=30) as executor:
        #     [threads.append(executor.submit(self.init_links, url)) for url in root_urls]
        #
        #     for i, task in enumerate(as_completed(threads)):
        #         print(f"ROOT {i} / {len(threads)}")
        #         result.extend(task.result())

    def scrape_all_products(self):
        with open(r'C:\Users\dklec\PycharmProjects\Zentrada\zentrada_eu\zentrada_urls.csv', 'r') as f:
            urls = f.readlines()
        urls = urls[1:]
        urls = [x.replace('"', '').replace('\n', '') for x in urls]

        for idx, url in enumerate(urls):
            self.scrape_product(url)
            print(f'{idx+1} / {len(urls)} |{self.status}')

        self.main_output()

    def load_urls(self):
        with open(r'C:\Users\dklec\PycharmProjects\Zentrada\files\urls1.txt', 'r') as f:
        # with open(r'C:\Users\dklec\PycharmProjects\Zentrada\zentrada_eu\zentrada_urls.csv', 'r') as f:
            urls = f.readlines()
        urls = [x.replace('"', '').replace('\n', '') for x in urls]
        return urls

    def scrape_all_prod_multithreading(self):
        urls = self.load_urls()

        threads, result = [], []

        with ThreadPoolExecutor(max_workers=10) as executor:
            [threads.append(executor.submit(self.scrape_product, url)) for url in urls]

            for i, task in enumerate(as_completed(threads)):
                print(f"{i} / {len(threads)} Logged: {self.proxy_status}")
                time.sleep(0.1)
                result.extend(task.result())

        self.main_output()

    def scrape_product(self, url):
        data = {}

        s = cloudscraper.create_scraper()
        proxy = self.load_proxies()
        resp = s.get(url, proxies=proxy)

        if 'Kamil Wiktor' in resp.text:

            # with open('working_proxy.txt', 'a') as f:
            #     f.write(f'{str(proxy)}\n')

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
                data[f'Image {idx+1}'] = x

            self.products_output(data)
            self.proxy_status = 'YES'
            return data

        else:
            self.proxy_status = 'NO'
            # with open('working_proxy.txt', 'a') as f:
            #     f.write(f'{str(proxy)}\n')
            return ['not', 'working']

    @staticmethod
    def load_proxies():
        with open(r'C:\Users\dklec\PycharmProjects\Zentrada\zentrada_eu\proxies.txt') as f:
            proxies = f.read()
            proxies = proxies.split("\n")
            proxies = [x for x in proxies if x]
            proxies = [f'{p.split(":")[2]}:{p.split(":")[3]}@{p.split(":")[0]}:{p.split(":")[1]}' for p in proxies]
            proxies = [{'http': f'http://{i}', 'https': f'https://{i}'} for i in proxies]
        return choice(proxies)


if __name__ == '__main__':
    Products()
