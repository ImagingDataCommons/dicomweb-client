import importlib.metadata

from dicomweb_client.api import DICOMwebClient, DICOMfileClient
from dicomweb_client.protocol import DICOMClient
from dicomweb_client.uri import URI, URISuffix, URIType

try:
    __version__ = importlib.metadata.version(__name__)
except importlib.metadata.PackageNotFoundError:
    __version__ = '0.0.0'

__all__ = [
    'DICOMClient',
    'DICOMfileClient',
    'DICOMwebClient',
    'URI',
    'URISuffix',
    'URIType',
]
