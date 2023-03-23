# downloader.py
from __future__ import annotations

import logging
import threading
from multiprocessing import cpu_count
from queue import Queue
from typing import Tuple, Callable, NoReturn

from numpy import ndarray, array
from pandas import Series

import py621dl.reader
from py621dl import __NUMERIC as NUMERIC
from py621dl.net import get_image


class E621Downloader:

    def __init__(self,
                 csv_reader: py621dl.reader.Reader,
                 /,
                 *,
                 image_transforms: Callable = None,
                 timeout: NUMERIC | Tuple[NUMERIC, NUMERIC] = 5,
                 retries: int = 3,
                 num_workers: int = cpu_count()) -> None:

        self.reader = csv_reader
        self.image_transforms = image_transforms
        self.batch_size = self.reader.batch_size
        self.timeout = timeout
        self.retries = retries
        self.num_workers = num_workers
        self.img_list = []
        self.processing_queue = Queue()
        self.threads = [threading.Thread(
            target=self.worker, args=(self.processing_queue,), daemon=True
        ).start() for _ in range(min(cpu_count(), self.batch_size))]
        
    def worker(self, src_queue: Queue) -> NoReturn:
        while True:
            md5 = src_queue.get(block=True)
            img = get_image(md5, timeout=self.timeout, retries=self.retries)
            self.img_list.append(img)
            src_queue.task_done()

    def __iter__(self) -> E621Downloader:
        return self

    def __next__(self) -> tuple[ndarray, list[Series]]:
        images = []
        rows = []
        try:
            while len(images) < self.batch_size:
                if images:
                    logging.info(f"Retrying {self.batch_size - len(images)} images")
                    batch = self.reader.get_rows(self.batch_size - len(images))
                else:
                    batch = next(self.reader)
                    
                for row in batch:
                    self.processing_queue.put(row['md5'])
                
                self.processing_queue.join()

                exc = [e for e in self.img_list if isinstance(e, Exception)]
                img = {i: img for i, img in enumerate(self.img_list) if isinstance(img, ndarray)}
                
                self.img_list = []

                if exc:
                    exc = ExceptionGroup("One or more downloads failed", exc)
                    logging.error(f'Failed to download images: {exc}\nAttempting to fill batch')

                images.extend(img.values())
                rows.extend([batch[i] for i in img.keys()])

            images = array(images)

            if self.image_transforms:
                images = self.image_transforms(images)

            return images, rows

        except StopIteration:
            raise StopIteration("No more rows to read")
