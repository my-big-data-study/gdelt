import hashlib
import os
import zipfile
from pathlib import Path

import requests


class GdeltV2CsvFile(object):
    def __init__(self, line):
        self.md5check = False
        fields = str(line).split(" ")
        assert 3 == len(fields)
        self.md5 = fields[1]
        self.url = fields[-1]
        self.filename = self.url.split("/")[-1]
        self.composed = True
        self.timestamp = self.filename.split('.')[0]
        self.zip_file_path = os.path.join('/data', self.timestamp, self.filename)
        self.csv_file = ''

    def get_zip_file(self):
        response = requests.get(self.url)
        Path(os.path.dirname(self.zip_file_path)).mkdir(parents=True, exist_ok=True)

        if response.status_code == requests.codes.ok:
            with open(self.zip_file_path, "wb") as f:
                file_hash = hashlib.md5()
                for chunk in response.iter_content(chunk_size=512):
                    f.write(chunk)
                    file_hash.update(chunk)

                # 验证下载文件的完整性
                if file_hash.hexdigest() == self.md5:
                    self.md5check = True
            if self.md5check is False:
                os.remove(self.zip_file_path)

    def unzip_csv(self):
        assert self.md5check
        self.csv_file = unzip(self.zip_file_path, os.path.dirname(self.zip_file_path))
        os.remove(self.zip_file_path)

    @property
    def get_csv_file(self):
        return self.csv_file


# 从 api 中解析文件
def parse_zip_list(url='http://data.gdeltproject.org/gdeltv2/lastupdate.txt'):
    response = requests.get(url)
    if response.status_code == requests.codes.ok:
        return list(map(lambda x: GdeltV2CsvFile(x), response.text.splitlines()))
    else:
        return None


# 解药zip文件
def unzip(source_path, dest_path):
    z_file = zipfile.ZipFile(source_path, "r")
    # ZipFile.namelist(): 获取ZIP文档内所有文件的名称列表
    namelist = z_file.namelist()
    assert 1 == len(namelist)
    z_file.extract(namelist[0], dest_path)
    z_file.close()
    return os.path.join(dest_path, namelist[0])


def downloader():
    gdelt_v2_csv_files = parse_zip_list()
    assert gdelt_v2_csv_files is not None
    for gdelt_v2_csv_file in gdelt_v2_csv_files:
        gdelt_v2_csv_file.get_zip_file()
        gdelt_v2_csv_file.unzip_csv()

    return list(map(lambda x: str(x.csv_file), gdelt_v2_csv_files))

# if __name__ == '__main__':
#     map = downloader()
#     print(map)
