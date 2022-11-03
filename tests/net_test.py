from urllib.request import urlopen

import pytest

from py621dl.net import *

MD5 = "3e47080200fbde2d7d2ccf419343ab0a"


@pytest.mark.dependency()
@pytest.mark.enable_socket
def test_url_valid():
    url = construct_e621_img_link(MD5)
    assert url == "https://static1.e621.net/data/3e/47/3e47080200fbde2d7d2ccf419343ab0a.jpg"
    assert urlopen(url).getcode() == 200


@pytest.mark.dependency(depends=["test_url_valid"])
@pytest.mark.enable_socket
def test_get_image():
    img = get_image(MD5)
    assert img is not None


def test_malformed_download():
    e = download_image_as_bytes("https://invalidurl.com")
    assert isinstance(e, ConnectionError)
