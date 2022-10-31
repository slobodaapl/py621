import asyncio

from numpy import ndarray

from py621.net import get_image
from py621.reader import Reader


class E621Downloader:

    def __init__(self, csv_file, /, excluded_tags=None, minimum_score=5, *, chunk_size=2000, checkpoint_file=None):
        self.reader = Reader(csv_file,
                             excluded_tags=excluded_tags,
                             minimum_score=minimum_score,
                             chunk_size=chunk_size,
                             checkpoint_file=checkpoint_file)

    def __iter__(self):
        return self

    def __next__(self) -> tuple[list[ndarray], list[str]]:
        batch = next(self.reader)
        images_async = [get_image(row['md5']) for row in batch]
        try:
            #  TODO: Figure out exceptions for asyncio and IO
            images = await asyncio.gather(*images_async)
        except Exception as e:
            raise e

        return images, [row['md5'] for row in batch]
