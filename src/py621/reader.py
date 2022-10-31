import json

import pandas as pd

from py621 import DATA_COLUMNS


class Reader:

    def __init__(self, csv_file, /, batch_size=2, excluded_tags=None, minimum_score=10, *, chunk_size=2000,
                 checkpoint_file=None):
        self.df_buffered = pd.read_csv(csv_file, chunksize=chunk_size, usecols=DATA_COLUMNS, iterator=True)
        self.batch_size = batch_size
        self.excluded_tags = frozenset(excluded_tags) if excluded_tags is not None else None
        self.minimum_score = minimum_score
        self.checkpoint_file = checkpoint_file
        self.df_gen = iter(self.__chunk_gen())
        self.chunk_idx = None
        self.row_gen = None
        self.row_idx = None

        self.__init()

    def __init(self):
        # check if checkpoint is None, if not try loading it as json
        if self.checkpoint_file is not None:
            with open(self.checkpoint_file, 'r') as f:
                chunk_idx_skip, row_idx_skip = json.load(f)

            # skip chunks
            while self.chunk_idx < chunk_idx_skip:
                self.chunk_idx, _ = next(self.df_gen)

            self.chunk_idx, chunk = next(self.df_gen)
            self.row_gen = iter(self.__row_gen(chunk))

            # skip rows
            while self.row_idx != row_idx_skip:
                self.row_idx, _ = next(self.row_gen)

        else:
            self.chunk_idx, self.chunk = next(self.df_gen)
            self.row_gen = iter(self.__row_gen(self.chunk))

    def __iter__(self):
        return self

    def __next__(self):
        batch = []
        for _ in range(self.batch_size):
            try:
                self.row_idx, row = next(self.row_gen)
                batch.append(row)
            except StopIteration:
                try:
                    self.chunk_idx, chunk = next(self.df_gen)
                    self.row_gen = iter(self.__row_gen(self.chunk))
                    self.row_idx, row = next(self.row_gen)
                    batch.append(row)
                except StopIteration:
                    break
        self.__save_checkpoint()
        return batch

    def __chunk_gen(self):
        for chunk_idx, chunk in enumerate(self.df_buffered):
            yield chunk_idx, chunk

    def __row_gen(self, chunk):
        for row_idx, row in chunk.iterrows():

            if self.__is_row_invalid(row):
                continue

            yield row_idx, row

    def __save_checkpoint(self):
        if self.checkpoint_file is not None:
            with open(self.checkpoint_file, 'w') as f:
                json.dump([self.chunk_idx, self.row_idx], f)

    def __is_row_invalid(self, row):
        is_invalid = False
        is_invalid |= self.excluded_tags and self.excluded_tags.intersection(row['tag_string'].split()) != set()
        is_invalid |= self.minimum_score > row['score']
        is_invalid |= row['is_deleted'] == 't'
        is_invalid |= row['is_flagged'] == 't'
        return is_invalid
