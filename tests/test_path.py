from dicomweb_client.path import Path, Type

import pytest

_STUDY_UID = '1.2.3'
_SERIES_UID = '4.5.6'
_INSTANCE_UID = '7.8.9'

# DICOMweb URLs constructed from the UIDs above.
_BASE_URL = 'https://lalalala.com'
_STUDY_URL = f'{_BASE_URL}/studies/{_STUDY_UID}'
_SERIES_URL = f'{_STUDY_URL}/series/{_SERIES_UID}'
_INSTANCE_URL = f'{_SERIES_URL}/instances/{_INSTANCE_UID}'


@pytest.mark.parametrize('illegal_char', ['/', '@', 'a', 'A'])
def test_uid_illegal_character(illegal_char):
    """Checks *ValueError* is raised when a UID contains an illegal char."""
    with pytest.raises(ValueError, match='must match'):
        Path(_BASE_URL, f'1.2{illegal_char}3')
    with pytest.raises(ValueError, match='must match'):
        Path(_BASE_URL, '1.2.3', f'4.5{illegal_char}6')
    with pytest.raises(ValueError, match='must match'):
        Path(_BASE_URL, '1.2.3', '4.5.6', f'7.8{illegal_char}9')


@pytest.mark.parametrize('illegal_uid', ['.23', '1.2..4', '1.2.', '.'])
def test_uid_illegal_format(illegal_uid):
    """Checks *ValueError* is raised if a UID is in an illegal format."""
    with pytest.raises(ValueError, match='must match'):
        Path(_BASE_URL, illegal_uid)
    with pytest.raises(ValueError, match='must match'):
        Path(_BASE_URL, '1.2.3', illegal_uid)
    with pytest.raises(ValueError, match='must match'):
        Path(_BASE_URL, '1.2.3', '4.5.6', illegal_uid)


def test_uid_length():
    """Checks that UIDs longer than 64 characters are disallowed."""
    # Success with 64 characters.
    uid_64 = '1' * 64
    Path(_BASE_URL, uid_64)
    Path(_BASE_URL, '1.2.3', uid_64)
    Path(_BASE_URL, '1.2.3', '4.5.6', uid_64)

    # Failure with 65 characters.
    uid_65 = '1' * 65
    with pytest.raises(ValueError, match='UID cannot have more'):
        Path(_BASE_URL, uid_65)
    with pytest.raises(ValueError, match='UID cannot have more'):
        Path(_BASE_URL, '1.2.3', uid_65)
    with pytest.raises(ValueError, match='UID cannot have more'):
        Path(_BASE_URL, '1.2.3', '4.5.6', uid_65)


def test_uid_missing_error():
    """Checks *ValueError* is raised when an expected UID is missing."""
    with pytest.raises(ValueError, match='study_uid missing with'):
        Path(_BASE_URL, None, '4.5.6')
    with pytest.raises(ValueError, match='study_uid missing with'):
        Path(_BASE_URL, None, '4.5.6', '7.8.9')
    with pytest.raises(ValueError, match='series_uid missing with'):
        Path(_BASE_URL, '4.5.6', None, '7.8.9')


def test_trailing_slash_error():
    """Tests constructor failure if the Service path has a trailing slash."""
    base_url = 'https://oh-well-this-was-fun.com/'
    with pytest.raises(ValueError, match='trailing forward slash'):
        Path(base_url)


def test_from_string_service_path():
    """Checks that Service path is parsed correctly and behaves as expected."""
    service_path = Path.from_string(_BASE_URL)
    assert service_path.base_url == _BASE_URL
    assert service_path.study_uid is None
    assert service_path.series_uid is None
    assert service_path.instance_uid is None
    assert service_path.type == Type.SERVICE
    assert str(service_path) == _BASE_URL
    assert str(service_path.base_subpath()) == _BASE_URL
    with pytest.raises(ValueError, match='Cannot get a Study path'):
        service_path.study_subpath()
    with pytest.raises(ValueError, match='Cannot get a Series path'):
        service_path.series_subpath()


def test_from_string_study_path():
    """Checks that Study path is parsed correctly and behaves as expected."""
    study_path = Path.from_string(_STUDY_URL)
    assert study_path.base_url == _BASE_URL
    assert study_path.study_uid == _STUDY_UID
    assert study_path.series_uid is None
    assert study_path.instance_uid is None
    assert study_path.type == Type.STUDY
    assert str(study_path) == _STUDY_URL
    assert str(study_path.base_subpath()) == _BASE_URL
    assert str(study_path.study_subpath()) == _STUDY_URL
    with pytest.raises(ValueError, match='Cannot get a Series path'):
        study_path.series_subpath()


def test_from_string_series_path():
    """Checks that Series path is parsed correctly and behaves as expected."""
    series_path = Path.from_string(_SERIES_URL)
    assert series_path.base_url == _BASE_URL
    assert series_path.study_uid == _STUDY_UID
    assert series_path.series_uid == _SERIES_UID
    assert series_path.instance_uid is None
    assert series_path.type == Type.SERIES
    assert str(series_path) == _SERIES_URL
    assert str(series_path.base_subpath()) == _BASE_URL
    assert str(series_path.study_subpath()) == _STUDY_URL
    assert str(series_path.series_subpath()) == _SERIES_URL


def test_from_string_instance_path():
    """Checks Instance path is parsed correctly and behaves as expected."""
    instance_path = Path.from_string(_INSTANCE_URL)
    assert instance_path.base_url == _BASE_URL
    assert instance_path.study_uid == _STUDY_UID
    assert instance_path.series_uid == _SERIES_UID
    assert instance_path.instance_uid == _INSTANCE_UID
    assert instance_path.type == Type.INSTANCE
    assert str(instance_path) == _INSTANCE_URL
    assert str(instance_path.base_subpath()) == _BASE_URL
    assert str(instance_path.study_subpath()) == _STUDY_URL
    assert str(instance_path.series_subpath()) == _SERIES_URL


@pytest.mark.parametrize(
    'resource_url',
    [f'{_BASE_URL}/studies/1.2.3/lolol/4.5.6',
     f'{_BASE_URL}/studies/1.2.3/series/4.5.6/lolol/7.8.9']
)
def test_from_string_invalid_resource_delimiter(resource_url):
    """Checks *ValueError* is raised if unexpected resource delimiter found."""
    with pytest.raises(ValueError, match='Error parsing the suffix'):
        Path.from_string(resource_url)


@pytest.mark.parametrize('service', ['', 'http://'])
def test_from_string_invalid(service):
    """Checks *ValueError* raised when the path string is invalid."""
    with pytest.raises(ValueError, match='Not an HTTPS URL'):
        Path.from_string(f'{service}invalid_url')


@pytest.mark.parametrize(
    'child,parent',
    [(Path.from_string(_BASE_URL), Path.from_string(_BASE_URL)),
     (Path.from_string(_STUDY_URL), Path.from_string(_BASE_URL)),
     (Path.from_string(_SERIES_URL), Path.from_string(_STUDY_URL)),
     (Path.from_string(_INSTANCE_URL), Path.from_string(_SERIES_URL))])
def test_parent(child, parent):
    """Validates the expected parent sub-path from `parent` attribute."""
    assert str(child.parent) == str(parent)


@pytest.mark.parametrize(
    'path,parts',
    [(Path.from_string(_BASE_URL), (_BASE_URL,)),
     (Path.from_string(_STUDY_URL), (_BASE_URL, _STUDY_UID)),
     (Path.from_string(_SERIES_URL), (_BASE_URL, _STUDY_UID, _SERIES_UID)),
     (Path.from_string(_INSTANCE_URL), (_BASE_URL, _STUDY_UID, _SERIES_UID,
                                        _INSTANCE_UID))])
def test_parts(path, parts):
    """Validates the expected parts from `parts` attribute."""
    assert str(path.parts) == str(parts)


def test_from_string_type_error():
    """Checks *ValueError* raised when the actual type does match expected."""
    for path_type in Type:
        if path_type != Type.SERVICE:
            with pytest.raises(ValueError, match='Unexpected path type'):
                Path.from_string(_BASE_URL, path_type)
        if path_type != Type.STUDY:
            with pytest.raises(ValueError, match='Unexpected path type'):
                Path.from_string(_STUDY_URL, path_type)
        if path_type != Type.SERIES:
            with pytest.raises(ValueError, match='Unexpected path type'):
                Path.from_string(_SERIES_URL, path_type)
        if path_type != Type.INSTANCE:
            with pytest.raises(ValueError, match='Unexpected path type'):
                Path.from_string(_INSTANCE_URL, path_type)
