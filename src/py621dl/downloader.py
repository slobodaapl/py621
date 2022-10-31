# downloader.py

import asyncio
import logging

from numpy import ndarray

import py621dl.reader
from py621dl.net import get_image


class E621Downloader:

    def __init__(self, csv_reader: py621dl.reader.Reader, /, *, timeout=5, retries=3):
        self.reader = csv_reader
        self.batch_size = self.reader.batch_size
        self.timeout = timeout
        self.retries = retries

    def __iter__(self):
        return self

    def __next__(self) -> tuple[list[ndarray], list[str]]:
        images = []
        rows = []
        try:
            while len(images) < self.batch_size:
                if images:
                    logging.info(f"Retrying {self.batch_size - len(images)} images")
                    batch = self.reader.get_rows(self.batch_size - len(images))
                else:
                    batch = next(self.reader)

                images_async = [get_image(row['md5'], self.timeout, self.retries) for row in batch]

                images_temp = await asyncio.gather(*images_async)
                exc = [e for e in images_temp if isinstance(e, Exception)]
                img = {i: img for i, img in enumerate(images_temp) if isinstance(i, ndarray)}

                if exc:
                    exc = ExceptionGroup("One or more downloads failed", exc)
                    logging.error(f'Failed to download images: {exc}\nAttempting to fill batch')

                images.extend(img.values())
                rows.extend([batch[i] for i in img.keys()])

            return images, rows
        except StopIteration:
            raise StopIteration("No more rows to read")
