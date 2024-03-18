import logging
from typing import Optional
from urllib.error import HTTPError, URLError
from urllib.request import urlretrieve
import progressbar


class MyProgressBar:
    def __init__(self):
        self.pbar = None

    def __call__(self, block_num, block_size, total_size):
        if not self.pbar:
            self.pbar = progressbar.ProgressBar(maxval=total_size)
            self.pbar.start()

        downloaded = block_num * block_size
        if downloaded < total_size:
            self.pbar.update(downloaded)
        else:
            self.pbar.finish()


class DownloadTnxDataset:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute(self, url: str, filename: str) -> Optional[int]:
        try:
            urlretrieve(url, filename, MyProgressBar())
        except HTTPError as e:
            self.logger.error(f'Download URL error: {e}')
            return -1
        except URLError as e:
            self.logger.error(f'Download URL error: {e}')
            return -1
        return None
