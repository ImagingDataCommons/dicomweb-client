"""Unit tests for `dicomweb_client.ext.gcp.uri` module."""
from dicomweb_client.ext.gcp.uri import GoogleCloudHealthcareURL

import pytest

_PROJECT_ID = 'my-project44'
_LOCATION = 'us-central1'
_DATASET_ID = 'my-44.dataset'
_DICOM_STORE_ID = 'my.d1com_store'
_CHC_API_URL = 'https://healthcare.googleapis.com/v1'
_CHC_BASE_URL = (
    f'{_CHC_API_URL}/'
    f'projects/{_PROJECT_ID}/locations/{_LOCATION}/'
    f'datasets/{_DATASET_ID}/dicomStores/{_DICOM_STORE_ID}/dicomWeb')


def test_chc_dicom_store_str():
    """Locks down `GoogleCloudHealthcareURL.__str__()`."""
    assert str(
        GoogleCloudHealthcareURL(
            _PROJECT_ID,
            _LOCATION,
            _DATASET_ID,
            _DICOM_STORE_ID)) == _CHC_BASE_URL


@pytest.mark.parametrize('name', ['hmmm.1', '#95', '43/'])
def test_chc_invalid_project_or_location(name):
    """Tests for bad `project_id`, `location`."""
    with pytest.raises(ValueError, match='project_id'):
        GoogleCloudHealthcareURL(name, _LOCATION, _DATASET_ID, _DICOM_STORE_ID)
    with pytest.raises(ValueError, match='location'):
        GoogleCloudHealthcareURL(
            _PROJECT_ID, name, _DATASET_ID, _DICOM_STORE_ID)


@pytest.mark.parametrize('name', ['hmmm.!', '#95', '43/'])
def test_chc_invalid_dataset_or_store(name):
    """Tests for bad `dataset_id`, `dicom_store_id`."""
    with pytest.raises(ValueError, match='dataset_id'):
        GoogleCloudHealthcareURL(_PROJECT_ID, _LOCATION, name, _DICOM_STORE_ID)
    with pytest.raises(ValueError, match='dicom_store_id'):
        GoogleCloudHealthcareURL(
            _PROJECT_ID, _LOCATION, _DATASET_ID, name)


@pytest.mark.parametrize('url', [f'{_CHC_API_URL}beta', 'https://some.url'])
def test_chc_from_string_invalid_api(url):
    """Tests for bad API URL error`GoogleCloudHealthcareURL.from_string()`."""
    with pytest.raises(ValueError, match='v1 URL'):
        GoogleCloudHealthcareURL.from_string(url)


@pytest.mark.parametrize('url', [
    f'{_CHC_BASE_URL}/',  # Trailing slash disallowed.
    f'{_CHC_API_URL}/project/p/locations/l/datasets/d/dicomStores/ds/dicomWeb',
    f'{_CHC_API_URL}/projects/p/location/l/datasets/d/dicomStores/ds/dicomWeb',
    f'{_CHC_API_URL}/projects/p/locations/l/dataset/d/dicomStores/ds/dicomWeb',
    f'{_CHC_API_URL}/projects/p/locations/l/datasets/d/dicomStore/ds/dicomWeb',
    f'{_CHC_API_URL}/locations/l/datasets/d/dicomStores/ds/dicomWeb',
    f'{_CHC_API_URL}/projects/p/datasets/d/dicomStores/ds/dicomWeb',
    f'{_CHC_API_URL}/projects/p/locations/l/dicomStores/ds/dicomWeb',
    f'{_CHC_API_URL}/projects/p/locations/l/datasets/d/dicomWeb',
    f'{_CHC_API_URL}/projects/p/locations/l//datasets/d/dicomStores/ds/dicomWeb'
])
def test_chc_from_string_invalid_store_name(url):
    """Tests for bad Store name `GoogleCloudHealthcareURL.from_string()`."""
    with pytest.raises(ValueError, match='v1 DICOM'):
        GoogleCloudHealthcareURL.from_string(url)


def test_chc_from_string_success():
    """Locks down `GoogleCloudHealthcareURL.from_string()`."""
    store = GoogleCloudHealthcareURL.from_string(_CHC_BASE_URL)
    assert store.project_id == _PROJECT_ID
    assert store.location == _LOCATION
    assert store.dataset_id == _DATASET_ID
    assert store.dicom_store_id == _DICOM_STORE_ID
