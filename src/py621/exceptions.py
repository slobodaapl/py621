# exceptions.py

class CVException(Exception):
    """Raised for color conversion and image decoding error from downloaded image."""

    def __init__(self):
        super().__init__("Failed to convert image to OpenCV format or convert color space")
