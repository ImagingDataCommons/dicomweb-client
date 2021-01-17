from dicomweb_client.uri import URI, URIType

import pytest

_STUDY_UID = '1.2.3'
_SERIES_UID = '4.5.6'
_INSTANCE_UID = '7.8.9'

# DICOMweb URLs constructed from the UIDs above.
_BASE_URL = 'https://lalalala.com'
_STUDY_URI = f'{_BASE_URL}/studies/{_STUDY_UID}'
_SERIES_URI = f'{_STUDY_URI}/series/{_SERIES_UID}'
_INSTANCE_URI = f'{_SERIES_URI}/instances/{_INSTANCE_UID}'


@pytest.mark.parametrize('illegal_char', ['/', '@', 'a', 'A'])
def test_uid_illegal_character(illegal_char):
    """Checks *ValueError* is raised when a UID contains an illegal char."""
    with pytest.raises(ValueError, match='must match'):
        URI(_BASE_URL, f'1.2{illegal_char}3')
    with pytest.raises(ValueError, match='must match'):
        URI(_BASE_URL, '1.2.3', f'4.5{illegal_char}6')
    with pytest.raises(ValueError, match='must match'):
        URI(_BASE_URL, '1.2.3', '4.5.6', f'7.8{illegal_char}9')


@pytest.mark.parametrize('illegal_uid', ['.23', '1.2..4', '1.2.', '.'])
def test_uid_illegal_format(illegal_uid):
    """Checks *ValueError* is raised if a UID is in an illegal format."""
    with pytest.raises(ValueError, match='must match'):
        URI(_BASE_URL, illegal_uid)
    with pytest.raises(ValueError, match='must match'):
        URI(_BASE_URL, '1.2.3', illegal_uid)
    with pytest.raises(ValueError, match='must match'):
        URI(_BASE_URL, '1.2.3', '4.5.6', illegal_uid)


def test_uid_length():
    """Checks that UIDs longer than 64 characters are disallowed."""
    # Success with 64 characters.
    uid_64 = '1' * 64
    URI(_BASE_URL, uid_64)
    URI(_BASE_URL, '1.2.3', uid_64)
    URI(_BASE_URL, '1.2.3', '4.5.6', uid_64)

    # Failure with 65 characters.
    uid_65 = '1' * 65
    with pytest.raises(ValueError, match='UID cannot have more'):
        URI(_BASE_URL, uid_65)
    with pytest.raises(ValueError, match='UID cannot have more'):
        URI(_BASE_URL, '1.2.3', uid_65)
    with pytest.raises(ValueError, match='UID cannot have more'):
        URI(_BASE_URL, '1.2.3', '4.5.6', uid_65)


def test_uid_missing_error():
    """Checks *ValueError* is raised when an expected UID is missing."""
    with pytest.raises(ValueError, match='study_instance_uid missing with'):
        URI(_BASE_URL, None, '4.5.6')
    with pytest.raises(ValueError, match='study_instance_uid missing with'):
        URI(_BASE_URL, None, '4.5.6', '7.8.9')
    with pytest.raises(ValueError, match='series_instance_uid missing with'):
        URI(_BASE_URL, '4.5.6', None, '7.8.9')


def test_trailing_slash_error():
    """Tests constructor failure if the Service URL has a trailing slash."""
    base_url = 'https://oh-well-this-was-fun.com/'
    with pytest.raises(ValueError, match='trailing forward slash'):
        URI(base_url)


def test_from_string_service_uri():
    """Checks that Service URL is parsed correctly and behaves as expected."""
    service_uri = URI.from_string(_BASE_URL)
    assert service_uri.base_url == _BASE_URL
    assert service_uri.study_instance_uid is None
    assert service_uri.series_instance_uid is None
    assert service_uri.sop_instance_uid is None
    assert service_uri.type == URIType.SERVICE
    assert str(service_uri) == _BASE_URL
    assert service_uri.base_url == _BASE_URL
    with pytest.raises(ValueError, match='Cannot get a Study URI'):
        service_uri.study_uri()
    with pytest.raises(ValueError, match='Cannot get a Series URI'):
        service_uri.series_uri()


def test_from_string_study_uri():
    """Checks that Study URI is parsed correctly and behaves as expected."""
    study_uri = URI.from_string(_STUDY_URI)
    assert study_uri.base_url == _BASE_URL
    assert study_uri.study_instance_uid == _STUDY_UID
    assert study_uri.series_instance_uid is None
    assert study_uri.sop_instance_uid is None
    assert study_uri.type == URIType.STUDY
    assert str(study_uri) == _STUDY_URI
    assert study_uri.base_url == _BASE_URL
    assert str(study_uri.study_uri()) == _STUDY_URI
    with pytest.raises(ValueError, match='Cannot get a Series URI'):
        study_uri.series_uri()


def test_from_string_series_uri():
    """Checks that Series URI is parsed correctly and behaves as expected."""
    series_uri = URI.from_string(_SERIES_URI)
    assert series_uri.base_url == _BASE_URL
    assert series_uri.study_instance_uid == _STUDY_UID
    assert series_uri.series_instance_uid == _SERIES_UID
    assert series_uri.sop_instance_uid is None
    assert series_uri.type == URIType.SERIES
    assert str(series_uri) == _SERIES_URI
    assert series_uri.base_url == _BASE_URL
    assert str(series_uri.study_uri()) == _STUDY_URI
    assert str(series_uri.series_uri()) == _SERIES_URI


def test_from_string_instance_uri():
    """Checks Instance URI is parsed correctly and behaves as expected."""
    instance_uri = URI.from_string(_INSTANCE_URI)
    assert instance_uri.base_url == _BASE_URL
    assert instance_uri.study_instance_uid == _STUDY_UID
    assert instance_uri.series_instance_uid == _SERIES_UID
    assert instance_uri.sop_instance_uid == _INSTANCE_UID
    assert instance_uri.type == URIType.INSTANCE
    assert str(instance_uri) == _INSTANCE_URI
    assert instance_uri.base_url == _BASE_URL
    assert str(instance_uri.study_uri()) == _STUDY_URI
    assert str(instance_uri.series_uri()) == _SERIES_URI


@pytest.mark.parametrize('resource_url', [
    f'{_BASE_URL}/studies/1.2.3/lolol/4.5.6',
    f'{_BASE_URL}/studies/1.2.3/series/4.5.6/lolol/7.8.9'
])
def test_from_string_invalid_resource_delimiter(resource_url):
    """Checks *ValueError* is raised if unexpected resource delimiter found."""
    with pytest.raises(ValueError, match='Error parsing the suffix'):
        URI.from_string(resource_url)


@pytest.mark.parametrize('service', ['', 'http://'])
def test_from_string_invalid(service):
    """Checks *ValueError* raised when the URI string is invalid."""
    with pytest.raises(ValueError, match='Not an HTTPS URI'):
        URI.from_string(f'{service}invalid_url')


@pytest.mark.parametrize(
    'child,parent',
    [(URI.from_string(_BASE_URL), URI.from_string(_BASE_URL)),
     (URI.from_string(_STUDY_URI), URI.from_string(_BASE_URL)),
     (URI.from_string(_SERIES_URI), URI.from_string(_STUDY_URI)),
     (URI.from_string(_INSTANCE_URI), URI.from_string(_SERIES_URI))])
def test_parent(child, parent):
    """Validates the expected parent URI from `parent` attribute."""
    assert str(child.parent) == str(parent)


@pytest.mark.parametrize(
    'uri,parts',
    [(URI.from_string(_BASE_URL), (_BASE_URL, )),
     (URI.from_string(_STUDY_URI), (_BASE_URL, _STUDY_UID)),
     (URI.from_string(_SERIES_URI), (_BASE_URL, _STUDY_UID, _SERIES_UID)),
     (URI.from_string(_INSTANCE_URI),
      (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID))])
def test_parts(uri, parts):
    """Validates the expected parts from `parts` attribute."""
    assert str(uri.parts) == str(parts)


@pytest.mark.parametrize('uri,hash_args', [
    (URI.from_string(_BASE_URL), (_BASE_URL, None, None, None)),
    (URI.from_string(_STUDY_URI), (_BASE_URL, _STUDY_UID, None, None)),
    (URI.from_string(_SERIES_URI), (_BASE_URL, _STUDY_UID, _SERIES_UID, None)),
    (URI.from_string(_INSTANCE_URI),
     (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID))
])
def test_hash(uri, hash_args):
    """Locks down the implementation of `__hash__()`."""
    assert hash(uri) == hash((*hash_args, ))


@pytest.mark.parametrize('uri,init_args', [
    (URI.from_string(_BASE_URL), (_BASE_URL, None, None, None)),
    (URI.from_string(_STUDY_URI), (_BASE_URL, _STUDY_UID, None, None)),
    (URI.from_string(_SERIES_URI), (_BASE_URL, _STUDY_UID, _SERIES_UID, None)),
    (URI.from_string(_INSTANCE_URI),
     (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID))
])
def test_repr(uri, init_args):
    """Locks down the implementation of `__repr__()`."""
    expected_repr = ('dicomweb_client.URI(base_url={}, study_instance_uid={}, '
                     'series_instance_uid={}, sop_instance_uid={})').format(
                         *(repr(arg) for arg in init_args))
    assert repr(uri) == expected_repr


@pytest.mark.parametrize('params', [
    (_BASE_URL, None, None, None),
    (_BASE_URL, _STUDY_UID, None, None),
    (_BASE_URL, _STUDY_UID, _SERIES_UID, None),
    (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID),
])
def test_eq_true(params):
    """Tests the `==` operator implementation for successful comparison."""
    assert URI(*params) == URI(*params)


@pytest.mark.parametrize('params', [
    (_BASE_URL, None, None, None),
    (_BASE_URL, _STUDY_UID, None, None),
    (_BASE_URL, _STUDY_UID, _SERIES_UID, None),
    (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID),
])
def test_eq_false(params):
    """Tests the `==` operator implementation for failed comparison."""
    other_params = (None if param is None else param + '1' for param in params)
    assert URI(*params) != URI(*other_params)


def test_from_string_type_error():
    """Checks *ValueError* raised when the actual type does match expected."""
    for uri_type in URIType:
        if uri_type != URIType.SERVICE:
            with pytest.raises(ValueError, match='Unexpected URI type'):
                URI.from_string(_BASE_URL, uri_type)
        if uri_type != URIType.STUDY:
            with pytest.raises(ValueError, match='Unexpected URI type'):
                URI.from_string(_STUDY_URI, uri_type)
        if uri_type != URIType.SERIES:
            with pytest.raises(ValueError, match='Unexpected URI type'):
                URI.from_string(_SERIES_URI, uri_type)
        if uri_type != URIType.INSTANCE:
            with pytest.raises(ValueError, match='Unexpected URI type'):
                URI.from_string(_INSTANCE_URI, uri_type)


@pytest.mark.parametrize('uri_args,update_args,expected_uri_args', [
    ((_BASE_URL, ), ('https://new', ), ('https://new', )),
    ((_BASE_URL, ), (None, '1'), (_BASE_URL, '1')),
    ((_BASE_URL, '1'), ('https://new', ), ('https://new', '1')),
    ((_BASE_URL, '1'), (None, '2'), (_BASE_URL, '2')),
    ((_BASE_URL, '1'), (None, None, '2'), (_BASE_URL, '1', '2')),
    ((_BASE_URL, '1', '2'), ('https://new', ), ('https://new', '1', '2')),
    ((_BASE_URL, '1', '2'), (None, '3'), (_BASE_URL, '3', '2')),
    ((_BASE_URL, '1', '2'), (None, None, '3'), (_BASE_URL, '1', '3')),
    ((_BASE_URL, '1', '2'), (None, None, None, '3'),
     (_BASE_URL, '1', '2', '3')),
    ((_BASE_URL, '1', '2', '3'), ('https://new', ),
     ('https://new', '1', '2', '3')),
    ((_BASE_URL, '1', '2', '3'), (None, '4'), (_BASE_URL, '4', '2', '3')),
    ((_BASE_URL, '1', '2', '3'), (None, None, '4'),
     (_BASE_URL, '1', '4', '3')),
    ((_BASE_URL, '1', '2', '3'), (None, None, None, '4'),
     (_BASE_URL, '1', '2', '4')),
])
def test_update(uri_args, update_args, expected_uri_args):
    """Tests for failure if the `URI` returned by `update()` is invalid."""
    actual_uri = URI(*uri_args).update(*update_args)
    expected_uri = URI(*expected_uri_args)
    assert actual_uri == expected_uri


@pytest.mark.parametrize('uri_args,update_args,error_msg', [
    ((_BASE_URL, ), (None, None, '1', None), 'study_instance_uid missing'),
    ((_BASE_URL, ), (None, None, None, '2'), 'study_instance_uid missing'),
    ((_BASE_URL, ), (None, None, '1', '2'), 'study_instance_uid missing'),
    ((_BASE_URL, _STUDY_UID),
     (None, None, None, '2'), 'series_instance_uid missing'),
])
def test_update_error(uri_args, update_args, error_msg):
    """Tests for failure if the `URI` returned by `update()` is invalid."""
    with pytest.raises(ValueError, match=error_msg):
        URI(*uri_args).update(*update_args)
