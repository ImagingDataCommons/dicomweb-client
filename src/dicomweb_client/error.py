'''Custom error classes'''
import requests


class DICOMJSONError(ValueError):
    '''Exception class for malformatted DICOM JSON.'''
    pass


class HTTPError(requests.exceptions.HTTPError):
    '''Exception class for HTTP requests with failure status codes.'''
    pass
