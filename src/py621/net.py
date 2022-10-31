# net.py

import io
import logging

import cv2
import numpy as np
import requests
from PIL import Image, UnidentifiedImageError
from requests.exceptions import HTTPError, ConnectionError, Timeout

from py621.exceptions import CVException


async def get_image(md5, timeout=5, retries=3):
    url = construct_e621_img_link(md5)
    download = download_image_as_bytes(url, timeout, retries)
    exc = [e for e in download if isinstance(e, Exception)]

    if exc:
        raise ExceptionGroup("Download failed", exc)

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
        # noinspection PyTypeChecker
        img = cv2.cvtColor(np.array(img), cv2.COLOR_RGB2BGR)
        img = cv2.imdecode(img, cv2.IMREAD_COLOR)
    except CVException as e:
        logging.error(f'Image decode failed: {e}')
        return e

    return img


def download_image_as_bytes(url, timeout=5, retries=3):
    exc = None
    for i in range(retries):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
        except HTTPError as e:
            logging.error(f'HTTP Error: {e}')
            return e
        except (ConnectionError, Timeout) as e:
            exc = e
            logging.error(f'Connection or Timeout Error: {e}\nRetrying attempt {i + 1} of {retries}')
        else:
            return requests.get(url).content
    else:
        logging.error(f'Failed to download image after {retries} attempts')
        return exc


def construct_e621_img_link(post_md5):
    return f'https://static1.e621.net/data/{post_md5[0:2]}/{post_md5[2:4]}/{post_md5}.jpg'
