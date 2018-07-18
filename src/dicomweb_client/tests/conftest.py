from pkg_resources import resource_filename

import pytest

from dicomweb_client.cli import _get_parser
from dicomweb_client.api import DICOMwebClient


@pytest.fixture
def parser():
    '''Instance of `argparse.Argparser`.'''
    return _get_parser()


@pytest.fixture
def cache_dir():
    '''Directory where responses are cached.'''
    return resource_filename('dicomweb_client', 'data')


@pytest.fixture
def client(httpserver):
    '''Instance of `dicomweb_client.api.DICOMwebClient`.'''
    return DICOMwebClient(httpserver.url)
