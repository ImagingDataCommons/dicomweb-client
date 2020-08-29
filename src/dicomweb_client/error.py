"""Custom error classes"""
import requests


class DICOMJSONError(ValueError):
    """Exception class for malformatted DICOM JSON."""
    pass
