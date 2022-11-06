from os import remove
from os.path import exists

import pytest

from py621dl.reader import Reader


@pytest.fixture
def cleanup():
    yield
    remove("resources/checkpoint.json")


@pytest.mark.order(1)
@pytest.mark.dependency()
def test_reader_repeatable():
    for batch_size in range(1, 3):
        for chunk_size in range(1, 4):
            reader = Reader("resources/test_chunk.csv", batch_size=batch_size, chunk_size=chunk_size, repeat=True)
            assert reader == iter(reader) is not None

            for _ in range(5):
                rows = next(reader)
                assert len(rows) == batch_size
                assert all(row is not None for row in rows)
                ids = [row["id"] for row in rows]
                assert len(ids) == len(set(ids))


@pytest.mark.order(2)
@pytest.mark.dependency(depends=["test_reader_repeatable"])
@pytest.mark.usefixtures("cleanup")
def test_reader_resumable():
    reader = Reader("resources/test_chunk.csv", batch_size=1, chunk_size=3,
                    repeat=True, checkpoint_file="resources/checkpoint.json")
    assert reader == iter(reader) is not None
    next(reader)
    next(reader)
    assert exists("resources/checkpoint.json")

    reader = Reader("resources/test_chunk.csv", batch_size=1, chunk_size=3,
                    repeat=True, checkpoint_file="resources/checkpoint.json")
    assert reader == iter(reader) is not None
    assert reader.get_row_idx() == 1
    next(reader)
    next(reader)

    reader = Reader("resources/test_chunk.csv", batch_size=1, chunk_size=3,
                    repeat=True, checkpoint_file="resources/checkpoint.json")
    assert reader == iter(reader) is not None
    assert reader.get_row_idx() == 3
    assert reader.get_chunk_idx() == 1


def test_reader_not_repeatable():
    reader = Reader("resources/test_chunk.csv", batch_size=3, chunk_size=4, repeat=False)
    assert reader == iter(reader) is not None
    next(reader)
    try:
        next(reader)
    except StopIteration:
        return
    else:
        assert False
