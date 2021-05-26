import os
import cloudscraper
import pandas as pd

import xlsxwriter

from lxml import html



class ListsInit(object):
    def __init__(self):
        super(ListsInit, self).__init__()

    # STATIC
    @staticmethod
    def products_output(data):
        products.append(data)


products = []



class DataWriter(ListsInit):
    def __init__(self):
        super(DataWriter, self).__init__()

    def main_output(self):
        dfs = {
            'Products': pd.DataFrame(products)
        }

        # file_path = r"\Desktop\spoke-london-output-done.xlsx"
        # app_dir = os.path.join(os.path.expanduser("~"))
        # writer = pd.ExcelWriter(app_dir + file_path, engine='xlsxwriter')
        writer = pd.ExcelWriter("zentrada-products-output.xlsx", engine='xlsxwriter')

        # Auto adjust column width, text wrap
        for sheet_name, df in dfs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            wb = writer.book
            worksheet = writer.sheets[sheet_name]
            text_format = wb.add_format({'text_wrap': True, 'valign': 'top'})

            for idx, col in enumerate(df):
                series = df[col]
                max_len = max((series.astype(str).map(len).max(),
                               len(str(series.name)))) + 1
                if max_len > 100:
                    worksheet.set_column(idx, idx, max_len / 3, text_format)
                else:
                    worksheet.set_column(idx, idx, max_len, text_format)

        writer.save()

    @staticmethod
    def clean_df(list_of_dicts):
        df = pd.DataFrame(list_of_dicts).apply(lambda x: pd.Series(x.dropna().values))
        return df


class Functions(object):
    def __init__(self):
        super(Functions, self).__init__()

    @staticmethod
    def clean_data(data):
        clean = [x.strip().replace('\n', '').replace('  ', '') for x in data]
        clean = list(filter(None, clean))
        clean = ', '.join(clean)
        return clean

    def extract(self, source, xpath):
        target = html.fromstring(source).xpath(xpath)
        target = self.clean_data(target)
        return target

    @staticmethod
    def get_response(url, auth, proxy):

        s = cloudscraper.create_scraper()
        r = s.get(url, params=auth, proxies=proxy)
        r.encoding = 'UTF-8'

        return r



