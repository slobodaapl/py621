# exceptions.py

from typing import Any


class CVException(Exception):
    """Raised for color conversion and image decoding error from downloaded image."""

    def __init__(self):
        super().__init__("Failed to convert image to OpenCV format or convert color space")


class ReaderException(Exception):
    """Raised for reader errors."""

    def __init__(self, message: str):
        super().__init__(message)


class InvalidRatingException(ReaderException):
    """Raised for invalid rating in CSV file."""

    def __init__(self, rating: Any):
        super().__init__(f"Invalid rating: {rating}. Only 's', 'q', and 'e' are allowed.")


class InvalidScoreException(ReaderException):
    """Raised for invalid score in CSV file."""

    def __init__(self):
        super().__init__(f"Invalid minimum score. Must be an integer.")
