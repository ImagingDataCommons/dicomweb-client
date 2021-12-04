__version__ = '0.54.2'

from dicomweb_client.api import DICOMwebClient, DICOMfileClient
from dicomweb_client.protocol import DICOMClient
from dicomweb_client.uri import URI, URISuffix, URIType

__all__ = [
    'DICOMClient',
    'DICOMfileClient',
    'DICOMwebClient',
    'URI',
    'URISuffix',
    'URIType',
]
