import pytest

from py621dl import E621Downloader, Reader


@pytest.mark.parametrize("batch_size", [2, 3])
@pytest.mark.enable_socket
def test_downloader(batch_size):
    reader = Reader("resources/test_chunk.csv", batch_size=batch_size, chunk_size=3, repeat=True)
    assert reader == iter(reader) is not None

    downloader = E621Downloader(reader)
    assert downloader == iter(downloader) is not None

    for _ in range(10):
        images, rows = next(downloader)
        assert len(images) == len(rows) == batch_size
        assert all(row is not None for row in rows)
        assert all(image is not None for image in images)
