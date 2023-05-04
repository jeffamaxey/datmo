import os
import requests
from io import open
try:
    to_unicode = unicode
except NameError:
    to_unicode = str


class S3RemoteFileDriver(object):
    def __init__(self):
        self.type = "remote-datmo"

    @staticmethod
    def upload(src_filepath, s3_presigned_url):
        if not os.path.isfile(src_filepath):
            raise Exception(f"Can't upload file: {src_filepath}, validation failed.")
        with open(src_filepath, "r") as f:
            data = f.read()
            res = requests.put(s3_presigned_url, data=data)
            if res.status_code != 200:
                raise Exception(f"Upload failed: {res.text}")

    @staticmethod
    def download(s3_presigned_url, dst_filepath):
        res = requests.get(s3_presigned_url, stream=True)
        if res.status_code != 200:
            raise Exception(f"Upload failed: {res.text}")
        with open(dst_filepath, "w") as handle:
            for block in res.iter_content(1024):
                handle.write(to_unicode(block))
