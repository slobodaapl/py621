import io

import cv2
import numpy as np
import requests
from PIL import Image


async def get_image(md5):
    url = construct_e621_img_link(md5)
    img = io.BytesIO(download_image_as_bytes(url))
    # noinspection PyTypeChecker
    img = cv2.cvtColor(np.array(Image.open(img)), cv2.COLOR_RGB2BGR)
    return cv2.imdecode(img, cv2.IMREAD_COLOR)


def download_image_as_bytes(url):
    return requests.get(url).content


def construct_e621_img_link(post_md5):
    return f'https://static1.e621.net/data/{post_md5[0:2]}/{post_md5[2:4]}/{post_md5}.jpg'
