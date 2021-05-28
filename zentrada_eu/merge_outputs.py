import re
import os
import pandas as pd
from natsort import natsorted

from zentrada_eu.main import DataWriter


class MergedOutput(DataWriter):
    def __init__(self):
        super(MergedOutput, self).__init__()
        self.file_path = r'C:\Users\dklec\PycharmProjects\Zentrada\outputs'
        self.__save_into_sheets()

    def __init_files(self):
        dir_list = natsorted(os.listdir(self.file_path))
        return dir_list

    def __init_dfs(self):
        files = self.__init_files()
        dfs = {}

        for idx, file in enumerate(files):
            data = pd.read_excel(rf'{self.file_path}\{file}', 'Products')
            dfs[f'Products {idx + 1}'] = data
            print(f'DataFrame {idx} created')

        return dfs

    def __save_into_sheets(self):
        dfs = self.__init_dfs()

        writer = pd.ExcelWriter("zentrada-products-output.xlsx", engine='xlsxwriter')

        sheets = 0
        for sheet_name, df in dfs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            self.adjust_column_size(df, writer, sheet_name)
            sheets+=1
            print(f'Sheet {sheets} done')

        print('Saving all data into file')
        writer.save()





if __name__ == '__main__':
    MergedOutput()
