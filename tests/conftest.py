from pathlib import Path

import pytest
from pydicom.data.data_manager import DATA_ROOT

from dicomweb_client.cli import _get_parser
from dicomweb_client.api import DICOMwebClient
from dicomweb_client.file import DICOMfileClient


@pytest.fixture
def parser():
    '''Instance of `argparse.Argparser`.'''
    return _get_parser()


@pytest.fixture
def cache_dir():
    '''Directory where responses are cached.'''
    return Path(__file__).parent.parent.joinpath('data')


@pytest.fixture
def client(httpserver):
    '''Instance of `dicomweb_client.api.DICOMwebClient`.'''
    return DICOMwebClient(httpserver.url)


@pytest.fixture
def file_client():
    '''Instance of `dicomweb_client.api.DICOMwebClient`.'''
    base_dir = Path(DATA_ROOT)
    return DICOMfileClient(base_dir, recreate_db=True)
