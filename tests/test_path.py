from dicomweb_path import Path, Type

import pytest

# Support alphanumeric characters in DICOM UIDs.
_STUDY_UID = '1.2.3a'
_SERIES_UID = '4.5.6b'
_INSTANCE_UID = '7.8.9c'

# DICOMweb URLs constructed from the UIDs above.
_SERVICE_URL = 'https://lalalala.com'
_STUDY_URL = f'{_SERVICE_URL}/studies/{_STUDY_UID}'
_SERIES_URL = f'{_STUDY_URL}/series/{_SERIES_UID}'
_INSTANCE_URL = f'{_SERIES_URL}/instances/{_INSTANCE_UID}'

# '/' is not allowed because the parsing logic in the class uses '/' to
# tokenize the path.
# '@' is not allowed due to a potential security vulnerability.


@pytest.mark.parametrize('illegal_char', ['/', '@'])
def test_no_forward_slash_or_at(illegal_char):
    """Checks *ValueError* is raised when an attribute contains '/' or '@'."""
    with pytest.raises(ValueError, match=r'\'study_uid\' must match'):
        Path(_SERVICE_URL, f'1.2{illegal_char}3')
    with pytest.raises(ValueError, match=r'\'series_uid\' must match'):
        Path(_SERVICE_URL, '1.2.3', f'4.5{illegal_char}6')
    with pytest.raises(ValueError, match=r'\'instance_uid\' must match'):
        Path(_SERVICE_URL, '1.2.3', '4.5.6', f'7.8{illegal_char}9')


def test_uid_missing_error():
    """Checks *ValueError* is raised when an expected UID is missing."""
    with pytest.raises(ValueError, match='study_uid missing with'):
        Path(_SERVICE_URL, None, '4.5.6')
    with pytest.raises(ValueError, match='study_uid missing with'):
        Path(_SERVICE_URL, None, '4.5.6', '7.8.9')
    with pytest.raises(ValueError, match='series_uid missing with'):
        Path(_SERVICE_URL, '4.5.6', None, '7.8.9')


def test_trailing_slash_error():
    """Tests constructor failure if the Service path has a trailing slash."""
    service_url = 'https://oh-well-this-was-fun.com/'
    with pytest.raises(ValueError, match='trailing forward slash'):
        Path(service_url)


def test_from_string_service_path():
    """Checks that Service path is parsed correctly and behaves as expected."""
    service_path = Path.from_string(_SERVICE_URL)
    assert service_path.service_url == _SERVICE_URL
    assert service_path.study_uid is None
    assert service_path.series_uid is None
    assert service_path.instance_uid is None
    assert service_path.type == Type.SERVICE
    assert str(service_path) == _SERVICE_URL
    assert str(service_path.get_service_path()) == _SERVICE_URL
    with pytest.raises(ValueError, match='Cannot get a study path'):
        service_path.get_study_path()
    with pytest.raises(ValueError, match='Cannot get a series path'):
        service_path.get_series_path()


def test_from_string_study_path():
    """Checks that Study path is parsed correctly and behaves as expected."""
    study_path = Path.from_string(_STUDY_URL)
    assert study_path.service_url == _SERVICE_URL
    assert study_path.study_uid == _STUDY_UID
    assert study_path.series_uid is None
    assert study_path.instance_uid is None
    assert study_path.type == Type.STUDY
    assert str(study_path) == _STUDY_URL
    assert str(study_path.get_service_path()) == _SERVICE_URL
    assert str(study_path.get_study_path()) == _STUDY_URL
    with pytest.raises(ValueError, match='Cannot get a series path'):
        study_path.get_series_path()


def test_from_string_series_path():
    """Checks that Series path is parsed correctly and behaves as expected."""
    series_path = Path.from_string(_SERIES_URL)
    assert series_path.service_url == _SERVICE_URL
    assert series_path.study_uid == _STUDY_UID
    assert series_path.series_uid == _SERIES_UID
    assert series_path.instance_uid is None
    assert series_path.type == Type.SERIES
    assert str(series_path) == _SERIES_URL
    assert str(series_path.get_service_path()) == _SERVICE_URL
    assert str(series_path.get_study_path()) == _STUDY_URL
    assert str(series_path.get_series_path()) == _SERIES_URL


def test_from_string_instance_path():
    """Checks Instance path is parsed correctly and behaves as expected."""
    instance_path = Path.from_string(_INSTANCE_URL)
    assert instance_path.service_url == _SERVICE_URL
    assert instance_path.study_uid == _STUDY_UID
    assert instance_path.series_uid == _SERIES_UID
    assert instance_path.instance_uid == _INSTANCE_UID
    assert instance_path.type == Type.INSTANCE
    assert str(instance_path) == _INSTANCE_URL
    assert str(instance_path.get_service_path()) == _SERVICE_URL
    assert str(instance_path.get_study_path()) == _STUDY_URL
    assert str(instance_path.get_series_path()) == _SERIES_URL


@pytest.mark.parametrize(
    'resource_url',
    [f'{_SERVICE_URL}/studies/1.2.3/lolol/4.5.6',
     f'{_SERVICE_URL}/studies/1.2.3/series/4.5.6/lolol/7.8.9']
)
def test_from_string_invalid_resource_delimiter(resource_url):
    """Checks *ValueError* is raised if unexpected resource delimiter found."""
    with pytest.raises(ValueError, match='Error parsing the suffix'):
        Path.from_string(resource_url)


@pytest.mark.parametrize('service', ['', 'http://'])
def test_from_string_invalid(service):
    """Checks *ValueError* raised when the path string is invalid."""
    with pytest.raises(ValueError, match='Not an HTTPS url'):
        Path.from_string(f'{service}invalid_url')


def test_from_string_type_error():
    """Checks *ValueError* raised when the actual type does match expected."""
    for path_type in Type:
        if path_type != Type.SERVICE:
            with pytest.raises(ValueError, match='Unexpected path type'):
                Path.from_string(_SERVICE_URL, path_type)
        if path_type != Type.STUDY:
            with pytest.raises(ValueError, match='Unexpected path type'):
                Path.from_string(_STUDY_URL, path_type)
        if path_type != Type.SERIES:
            with pytest.raises(ValueError, match='Unexpected path type'):
                Path.from_string(_SERIES_URL, path_type)
        if path_type != Type.INSTANCE:
            with pytest.raises(ValueError, match='Unexpected path type'):
                Path.from_string(_INSTANCE_URL, path_type)
