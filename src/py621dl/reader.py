# reader.py

import json
from os.path import exists

import pandas as pd

DATA_COLUMNS = ["id", "md5", "rating", "image_width", "image_height", "tag_string", "score", "is_deleted", "is_flagged"]


class Reader:

    def __init__(self, csv_file, /, batch_size=2, excluded_tags=None, minimum_score=10, *, chunk_size=2000,
                 checkpoint_file=None, repeat=True):
        self.__csv = csv_file
        self.__checkpoint_file = checkpoint_file
        self.__repeat = repeat

        self.__excluded_tags = frozenset(excluded_tags) if excluded_tags is not None else None
        self.__minimum_score = minimum_score

        self.__df_buffered = pd.read_csv(csv_file, chunksize=chunk_size, usecols=DATA_COLUMNS, iterator=True)
        self.__chunk_iter = None
        self.__row_iter = None

        self.__chunk_idx = None
        self.__row_idx = None

        self.batch_size = batch_size

        self.__init()

    def get_rows(self, n_rows=1):
        batch = []
        while len(batch) < n_rows:
            try:
                self.__row_idx, row = next(self.__row_iter)
                batch.append(row)
            except StopIteration:
                try:
                    self.__chunk_idx, chunk = next(self.__chunk_iter)
                except StopIteration:
                    if self.__repeat:
                        self.__df_buffered = pd.read_csv(self.__csv, chunksize=2000,
                                                         usecols=DATA_COLUMNS, iterator=True)
                        if self.__checkpoint_file is not None:
                            with open(self.__checkpoint_file, 'w') as f:
                                json.dump([0, 0], f)
                        self.__init(repeat_init=True)
                        continue
                    else:
                        raise StopIteration
                else:
                    self.__row_iter = iter(self.__row_gen(chunk))
                    continue
        self.__save_checkpoint()
        return batch

    def get_row_idx(self):
        return self.__row_idx

    def get_chunk_idx(self):
        return self.__chunk_idx

    def __init(self, repeat_init=False):
        self.__chunk_iter = iter(self.__chunk_gen())
        # check if checkpoint is None, if not try loading it as json
        if self.__checkpoint_file is not None and exists(self.__checkpoint_file) and not repeat_init:
            with open(self.__checkpoint_file, 'r') as f:
                chunk_idx_skip, row_idx_skip = json.load(f)

            self.__chunk_idx, chunk = next(self.__chunk_iter)
            self.__row_idx = 0

            # skip chunks
            while self.__chunk_idx < chunk_idx_skip:
                self.__chunk_idx, chunk = next(self.__chunk_iter)

            self.__row_iter = iter(self.__row_gen(chunk))

            # skip rows
            while self.__row_idx != row_idx_skip:
                self.__row_idx, _ = next(self.__row_iter)

        else:
            self.__chunk_idx, self.chunk = next(self.__chunk_iter)
            self.__row_iter = iter(self.__row_gen(self.chunk))

    def __chunk_gen(self):
        for chunk_idx, chunk in enumerate(self.__df_buffered):
            yield chunk_idx, chunk

    def __row_gen(self, chunk):
        for row_idx, row in chunk.iterrows():

            if self.__is_row_invalid(row):
                continue

            yield row_idx, row

    def __save_checkpoint(self):
        if self.__checkpoint_file is not None:
            with open(self.__checkpoint_file, 'w') as f:
                json.dump([self.__chunk_idx, self.__row_idx], f)

    def __is_row_invalid(self, row):
        is_invalid = False
        is_invalid |= bool(self.__excluded_tags) and self.__excluded_tags.intersection(
            row['tag_string'].split()) != set()
        is_invalid |= self.__minimum_score > row['score']
        is_invalid |= row['is_deleted'] == 't'
        is_invalid |= row['is_flagged'] == 't'
        return is_invalid

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self.get_rows(self.batch_size)
        except StopIteration:
            raise StopIteration
