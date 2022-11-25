# downloader.py
from __future__ import annotations

import functools
import logging
import multiprocessing
from typing import Tuple

from numpy import ndarray
from pandas import Series

import py621dl.reader
from py621dl import __NUMERIC as NUMERIC
from py621dl.net import get_image


class E621Downloader:

    def __init__(self,
                 csv_reader: py621dl.reader.Reader,
                 /,
                 *,
                 timeout: NUMERIC | Tuple[NUMERIC, NUMERIC] = 5,
                 retries: int = 3):

        self.reader = csv_reader
        self.batch_size = self.reader.batch_size
        self.timeout = timeout
        self.retries = retries

    def __iter__(self) -> E621Downloader:
        return self

    def __next__(self) -> tuple[list[ndarray], list[Series]]:
        images = []
        rows = []
        try:
            while len(images) < self.batch_size:
                if images:
                    logging.info(f"Retrying {self.batch_size - len(images)} images")
                    batch = self.reader.get_rows(self.batch_size - len(images))
                else:
                    batch = next(self.reader)

                if self.batch_size > 2:
                    with multiprocessing.Pool(processes=min(multiprocessing.cpu_count(), self.batch_size)) as pool:
                        images_temp = pool.map_async(
                            functools.partial(get_image, timeout=self.timeout, retries=self.retries),
                            [row['md5'] for row in batch]
                        )

                        images_temp.wait()
                        images_temp = images_temp.get()
                else:
                    images_temp = [get_image(row['md5'], timeout=self.timeout, retries=self.retries) for row in batch]

                exc = [e for e in images_temp if isinstance(e, Exception)]
                img = {i: img for i, img in enumerate(images_temp) if isinstance(img, ndarray)}

                if exc:
                    exc = ExceptionGroup("One or more downloads failed", exc)
                    logging.error(f'Failed to download images: {exc}\nAttempting to fill batch')

                images.extend(img.values())
                rows.extend([batch[i] for i in img.keys()])

            return images, rows
        except StopIteration:
            raise StopIteration("No more rows to read")
