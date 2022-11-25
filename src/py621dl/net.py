# net.py

import io
import logging
from typing import Tuple, Union

import cv2
import numpy as np
import requests
from PIL import Image, UnidentifiedImageError
from requests.exceptions import HTTPError, ConnectionError, Timeout

from py621dl import __NUMERIC as NUMERIC
from py621dl.exceptions import CVException


# noinspection PyTypeChecker, PyUnresolvedReferences
def get_image(md5: str,
              /,
              timeout: NUMERIC | Tuple[NUMERIC, NUMERIC] = 5,
              retries: int = 3,
              *,
              __is_test_target: bool = False
              ) -> Union[Image.Image, Exception]:
    url = construct_e621_img_link(md5)

    if url == "test":
        return np.zeros((100, 100, 3), dtype=np.uint8)  # for testing

    download = download_image_as_bytes(url, timeout, retries)

    if isinstance(download, Exception):
        raise download

    try:
        img = io.BytesIO(download)
    except OSError as e:
        logging.error(f'Failed to convert bytes to image: {e}')
        return e

    try:
        img = Image.open(img)
    except UnidentifiedImageError as e:
        logging.error(f'Failed to identify image format from url: {url}')
        return e

    try:
        img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
    except CVException as e:
        logging.error(f'Image decode failed: {e}')
        return e

    return img


def download_image_as_bytes(url: str,
                            timeout: NUMERIC | Tuple[NUMERIC, NUMERIC] = 5,
                            retries: int = 3):
    exc = None
    for i in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
        except HTTPError as e:
            logging.error(f'HTTP Error: {e}')
            return e
        except (ConnectionError, Timeout) as e:
            logging.error(f'Connection or Timeout Error: {e}\nRetrying attempt {i + 1} of {retries}')
            return e
        else:
            return requests.get(url).content
    else:
        logging.error(f'Failed to download image after {retries} attempts')
        return exc


def construct_e621_img_link(post_md5: str):
    if post_md5 == "test":
        return post_md5

    return f'https://static1.e621.net/data/{post_md5[0:2]}/{post_md5[2:4]}/{post_md5}.jpg'
