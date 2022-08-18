from pathlib import Path

import pytest
from pydicom import config as pydicom_config
from pydicom.data.data_manager import DATA_ROOT

from dicomweb_client.cli import _get_parser
from dicomweb_client.api import DICOMwebClient
from dicomweb_client.file import DICOMfileClient


pydicom_config.settings.reading_validation_mode = pydicom_config.WARN
pydicom_config.settings.writing_validation_mode = pydicom_config.WARN


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
    url = f'file://{DATA_ROOT}'
    return DICOMfileClient(url, recreate_db=True, in_memory=True)
