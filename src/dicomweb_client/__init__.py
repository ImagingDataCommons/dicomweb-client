__version__ = '0.56.1'

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
