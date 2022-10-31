# py621dl - an iterable E621 downloader

This package is meant to be used in deep learning applications and automation,
not as a means to download specific images and post IDs or searching for tags.
For that application, please check out [py621](https://pypi.org/project/py621/)
which is not related to this package in any way.

The package is meant to be used with the official db export format from E621,
posts information. See [here](https://e621.net/db_export/) for available db exports
and [here](https://e621.net/help/api) for general information on the API.

!! This is a pre-release version, and is not meant for production use !!

Proper documentation, tests, and automated updates to the package will be added later.

## Installation

You can install the package using `pip install py621dl` on `python>=3.11`

## Usage

The E621Downloader class must be initialized using the Reader class, to which
the csv file must be passed. The Reader supports only the official db export csv files
of the format "posts-YYYY-MM-DD.csv.gz", either compressed or uncompressed.

The [E621Downloader](https://github.com/slobodaapl/py621dl/blob/90228111c166926f63f0cf3b991d9c82ca79e6e8/src/py621dl/downloader.py#L12-L14)
class can be initialized with the following parameters:

- `csv_reader`:
  the [Reader](https://github.com/slobodaapl/py621dl/blob/90228111c166926f63f0cf3b991d9c82ca79e6e8/src/py621dl/reader.py#L10-L13)
  object
- `timeout`: the timeout for the requests, in seconds
- `retries`: the number of retries for the requests

It can be used as an iterable, yielding lists of `np.ndarray` objects of the images. The list size
will depend on your `batch_size` specified for `Reader`. The images are of opencv BGR format.
The downloader automatically handles and filters deleted or flagged posts, and will attempt to fill
the batch with new images so that it will always yield a full batch.

The [Reader](https://github.com/slobodaapl/py621dl/blob/90228111c166926f63f0cf3b991d9c82ca79e6e8/src/py621dl/reader.py#L10-L13)
class can be initialized with the following parameters:

- `csv_file`: the path to the csv file
- `batch_size`: the size of the batch to be returned by the `E621Downloader`
- `excluded_tags`: a list of E621 tags to be excluded from the results
- `minimum_score`: the minimum score of the posts to be included in the results
- `chunk_size`: the size of the chunk to be read from the csv file at once
- `checkpoint_file`: the path to the checkpoint file, to resume from any point. If path doesn't exist, a new file will
  be created.
- `repeat`: whether to repeat from the beginning of the csv file when the end is reached automatically.
  Otherwise `StopIteration` is raised.
  `E621Downloader` handles this exception and raises its own `StopIteration` when the end is reached.

# Example use

```python
from py621dl import Reader, E621Downloader

reader = Reader("posts-2022-10-30.csv.gz")
downloader = E621Downloader(reader, timeout=10, retries=3)

for batch in downloader:
    # do something with the batch
    pass
```
