# reader.py

import json

import pandas as pd

from py621 import DATA_COLUMNS


class Reader:

    def __init__(self, csv_file, /, batch_size=2, excluded_tags=None, minimum_score=10, *, chunk_size=2000,
                 checkpoint_file=None, repeat=True):
        self.__csv = csv_file
        self.__checkpoint_file = checkpoint_file
        self.__repeat = repeat

        self.__excluded_tags = frozenset(excluded_tags) if excluded_tags is not None else None
        self.__minimum_score = minimum_score

        self.__df_buffered = pd.read_csv(csv_file, chunksize=chunk_size, usecols=DATA_COLUMNS, iterator=True)
        self.__chunk_iter = iter(self.__chunk_gen())
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
                        self.__init()
                        batch.append(next(self.__row_iter))
                    else:
                        raise StopIteration
                else:
                    self.__row_iter = iter(self.__row_gen(chunk))
                    batch.append(next(self.__row_iter))
        self.__save_checkpoint()
        return batch

    def __init(self):
        # check if checkpoint is None, if not try loading it as json
        if self.__checkpoint_file is not None:
            with open(self.__checkpoint_file, 'r') as f:
                chunk_idx_skip, row_idx_skip = json.load(f)

            # skip chunks
            while self.__chunk_idx < chunk_idx_skip:
                self.__chunk_idx, _ = next(self.__chunk_iter)

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
        is_invalid |= self.__excluded_tags and self.__excluded_tags.intersection(row['tag_string'].split()) != set()
        is_invalid |= self.__minimum_score > row['score']
        is_invalid |= row['is_deleted'] == 't'
        is_invalid |= row['is_flagged'] == 't'
        return is_invalid

    def __iter__(self):
        return self

    def __next__(self):
        return self.get_rows(self.batch_size)
