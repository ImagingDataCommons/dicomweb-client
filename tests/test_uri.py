from dicomweb_client.uri import URI, URISuffix, URIType

import pytest

_STUDY_UID = '1.2.3'
_SERIES_UID = '4.5.6'
_INSTANCE_UID = '7.8.9'
_FRAMES = (3, 4, 5)

# DICOMweb URLs constructed from the UIDs above.
_BASE_URL = 'https://lalalala.com'
_STUDY_URI = f'{_BASE_URL}/studies/{_STUDY_UID}'
_SERIES_URI = f'{_STUDY_URI}/series/{_SERIES_UID}'
_INSTANCE_URI = f'{_SERIES_URI}/instances/{_INSTANCE_UID}'
_FRAME_URI = f'{_INSTANCE_URI}/frames/{",".join(str(f) for f in _FRAMES)}'


@pytest.mark.parametrize('illegal_char', ['/', '@', 'a', 'A'])
def test_uid_illegal_character(illegal_char):
    """Checks *ValueError* is raised when a UID contains an illegal char."""
    with pytest.raises(ValueError, match='in conformance'):
        URI(_BASE_URL, f'1.2{illegal_char}3')
    with pytest.raises(ValueError, match='in conformance'):
        URI(_BASE_URL, '1.2.3', f'4.5{illegal_char}6')
    with pytest.raises(ValueError, match='in conformance'):
        URI(_BASE_URL, '1.2.3', '4.5.6', f'7.8{illegal_char}9')


@pytest.mark.parametrize('illegal_uid', ['.23', '1.2..4', '1.2.', '.'])
def test_uid_illegal_format(illegal_uid):
    """Checks *ValueError* is raised if a UID is in an illegal format."""
    with pytest.raises(ValueError, match='in conformance'):
        URI(_BASE_URL, illegal_uid)
    with pytest.raises(ValueError, match='in conformance'):
        URI(_BASE_URL, '1.2.3', illegal_uid)
    with pytest.raises(ValueError, match='in conformance'):
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


@pytest.mark.parametrize('uid', ['13-abc', 'hello', 'is it', 'me you\'re'])
def test_uid_permissive_valid(uid):
    """Tests valid "permissive" UIDs are accommodated iff the flag is set."""
    with pytest.raises(ValueError, match='in conformance'):
        URI(_BASE_URL, uid, permissive=False)
    URI(_BASE_URL, uid, permissive=True)


@pytest.mark.parametrize('uid', ['1.23.5@', '1/23.4'])
def test_uid_permissive_invalid(uid):
    """Tests that invalid "permissive" UIDs are rejected."""
    with pytest.raises(ValueError, match='Permissive mode'):
        URI(_BASE_URL, uid, permissive=True)


def test_uid_missing_error():
    """Checks *ValueError* is raised when an expected UID is missing."""
    with pytest.raises(ValueError, match='`study_instance_uid` missing with'):
        URI(_BASE_URL, None, '4.5.6')
    with pytest.raises(ValueError, match='`study_instance_uid` missing with'):
        URI(_BASE_URL, None, '4.5.6', '7.8.9')
    with pytest.raises(ValueError, match='`series_instance_uid` missing with'):
        URI(_BASE_URL, '4.5.6', None, '7.8.9')
    with pytest.raises(ValueError, match='`sop_instance_uid` missing with'):
        URI(_BASE_URL, '4.5.6', '7.8.9', None, _FRAMES)


def test_suffix_not_compatible():
    """Checks *ValueError* is raised when an incompatible suffix is set."""
    # Metadata.
    with pytest.raises(ValueError, match='\'metadata\'> suffix may only be'):
        URI(_BASE_URL, suffix=URISuffix.METADATA)
    with pytest.raises(ValueError, match='\'metadata\'> suffix may only be'):
        URI(_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID, _FRAMES,
            suffix=URISuffix.METADATA)
    # Rendered.
    with pytest.raises(ValueError, match='\'rendered\'> suffix requires a'):
        URI(_BASE_URL, suffix=URISuffix.RENDERED)
    # Thumbnail.
    with pytest.raises(ValueError, match='\'thumbnail\'> suffix requires a'):
        URI(_BASE_URL, suffix=URISuffix.THUMBNAIL)


@pytest.mark.parametrize('illegal_frame_number', [-2, -1, 0])
def test_non_positive_frame_numbers(illegal_frame_number):
    """Checks *ValueError* is raised if frame numbers are not positive."""
    with pytest.raises(ValueError, match='must be positive'):
        URI(_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID,
            [illegal_frame_number])


def test_frames_empty():
    """Checks *ValueError* is raised if frame list is empty."""
    with pytest.raises(ValueError, match='cannot be empty'):
        URI(_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID, [])


def test_frames_ascending_order():
    """Checks *ValueError* is raised if frames are not in ascending order."""
    with pytest.raises(ValueError, match='must be in ascending'):
        URI(_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID, [4, 3, 5])


def test_trailing_slash_error():
    """Tests constructor failure if the Service URL has a trailing slash."""
    base_url = 'https://oh-well-this-was-fun.com/'
    with pytest.raises(ValueError, match='trailing forward slash'):
        URI(base_url)


@pytest.mark.parametrize('base_url', [_BASE_URL, 'http://unsecured-http.com'])
def test_from_string_service_uri_protocols(base_url):
    """Checks that Service URL permits both HTTP and HTTPs protocols."""
    service_uri = URI.from_string(base_url)
    assert service_uri.base_url == base_url


def test_from_string_service_uri():
    """Checks that Service URL is parsed correctly and behaves as expected."""
    service_uri = URI.from_string(_BASE_URL)
    # Properties.
    assert service_uri.base_url == _BASE_URL
    assert service_uri.study_instance_uid is None
    assert service_uri.series_instance_uid is None
    assert service_uri.sop_instance_uid is None
    assert service_uri.type == URIType.SERVICE
    assert service_uri.suffix is None
    # String representation.
    assert str(service_uri) == _BASE_URL
    assert str(service_uri.base_uri()) == _BASE_URL
    # Constructor.
    assert str(service_uri) == str(URI(_BASE_URL))

    with pytest.raises(ValueError, match='Cannot get a Study URI'):
        service_uri.study_uri()
    with pytest.raises(ValueError, match='Cannot get a Series URI'):
        service_uri.series_uri()
    with pytest.raises(ValueError, match='Cannot get an Instance URI'):
        service_uri.instance_uri()
    with pytest.raises(ValueError, match='Cannot get a Frame URI'):
        service_uri.frame_uri()


@pytest.mark.parametrize(
    'suffix',
    [None, URISuffix.METADATA, URISuffix.RENDERED, URISuffix.THUMBNAIL])
def test_from_string_study_uri(suffix):
    """Checks that Study URI is parsed correctly and behaves as expected."""
    uri = _STUDY_URI if suffix is None else f'{_STUDY_URI}/{suffix.value}'
    study_uri = URI.from_string(uri)
    # Properties.
    assert study_uri.base_url == _BASE_URL
    assert study_uri.study_instance_uid == _STUDY_UID
    assert study_uri.series_instance_uid is None
    assert study_uri.sop_instance_uid is None
    assert study_uri.type == URIType.STUDY
    assert study_uri.suffix == suffix
    # String representation.
    assert str(study_uri) == uri
    assert str(study_uri.base_uri()) == _BASE_URL
    assert str(study_uri.study_uri()) == _STUDY_URI

    with pytest.raises(ValueError, match='Cannot get a Series URI'):
        study_uri.series_uri()
    with pytest.raises(ValueError, match='Cannot get an Instance URI'):
        study_uri.instance_uri()
    with pytest.raises(ValueError, match='Cannot get a Frame URI'):
        study_uri.frame_uri()


@pytest.mark.parametrize(
    'suffix',
    [None, URISuffix.METADATA, URISuffix.RENDERED, URISuffix.THUMBNAIL])
def test_from_string_series_uri(suffix):
    """Checks that Series URI is parsed correctly and behaves as expected."""
    uri = _SERIES_URI if suffix is None else f'{_SERIES_URI}/{suffix.value}'
    series_uri = URI.from_string(uri)
    # Properties.
    assert series_uri.base_url == _BASE_URL
    assert series_uri.study_instance_uid == _STUDY_UID
    assert series_uri.series_instance_uid == _SERIES_UID
    assert series_uri.sop_instance_uid is None
    assert series_uri.type == URIType.SERIES
    assert series_uri.suffix == suffix
    # String representation.
    assert str(series_uri) == uri
    assert str(series_uri.base_uri()) == _BASE_URL
    assert str(series_uri.study_uri()) == _STUDY_URI
    assert str(series_uri.series_uri()) == _SERIES_URI

    with pytest.raises(ValueError, match='Cannot get an Instance URI'):
        series_uri.instance_uri()
    with pytest.raises(ValueError, match='Cannot get a Frame URI'):
        series_uri.frame_uri()


@pytest.mark.parametrize(
    'suffix',
    [None, URISuffix.METADATA, URISuffix.RENDERED, URISuffix.THUMBNAIL])
def test_from_string_instance_uri(suffix):
    """Checks Instance URI is parsed correctly and behaves as expected."""
    uri = _INSTANCE_URI if suffix is None else f'{_INSTANCE_URI}/{suffix.value}'
    instance_uri = URI.from_string(uri)
    # Properties.
    assert instance_uri.base_url == _BASE_URL
    assert instance_uri.study_instance_uid == _STUDY_UID
    assert instance_uri.series_instance_uid == _SERIES_UID
    assert instance_uri.sop_instance_uid == _INSTANCE_UID
    assert instance_uri.type == URIType.INSTANCE
    assert instance_uri.suffix == suffix
    # String representation.
    assert str(instance_uri) == uri
    assert str(instance_uri.base_uri()) == _BASE_URL
    assert str(instance_uri.study_uri()) == _STUDY_URI
    assert str(instance_uri.series_uri()) == _SERIES_URI

    with pytest.raises(ValueError, match='Cannot get a Frame URI'):
        instance_uri.frame_uri()


@pytest.mark.parametrize(
    'suffix', [None, URISuffix.RENDERED, URISuffix.THUMBNAIL])
def test_from_string_frame_uri(suffix):
    """Checks frame numbers are parsed correctly and behaves as expected."""
    uri = _FRAME_URI if suffix is None else f'{_FRAME_URI}/{suffix.value}'
    frame_uri = URI.from_string(uri)
    # Properties.
    assert frame_uri.base_url == _BASE_URL
    assert frame_uri.study_instance_uid == _STUDY_UID
    assert frame_uri.series_instance_uid == _SERIES_UID
    assert frame_uri.sop_instance_uid == _INSTANCE_UID
    assert frame_uri.frames == _FRAMES
    assert frame_uri.type == URIType.FRAME
    assert frame_uri.suffix == suffix
    # String representation.
    assert str(frame_uri) == uri
    assert str(frame_uri.base_uri()) == _BASE_URL
    assert str(frame_uri.study_uri()) == _STUDY_URI
    assert str(frame_uri.series_uri()) == _SERIES_URI
    assert str(frame_uri.instance_uri()) == _INSTANCE_URI
    assert str(frame_uri.frame_uri()) == _FRAME_URI


@pytest.mark.parametrize('resource_url', [
    f'{_BASE_URL}/studies/1.2.3/lolol/4.5.6',
    f'{_BASE_URL}/studies/1.2.3/series/4.5.6/lolol/7.8.9'
])
def test_from_string_invalid_resource_delimiter(resource_url):
    """Checks *ValueError* is raised if unexpected resource delimiter found."""
    with pytest.raises(ValueError, match='Error parsing the suffix'):
        URI.from_string(resource_url)


@pytest.mark.parametrize('frame_uri', [
    f'{_BASE_URL}/studies/1.2.3/series/4.5.6/instances/7.8/frames/9,10.0',
    f'{_BASE_URL}/studies/1.2.3/series/4.5.6/instances/7.8/frames/9,a',
])
def test_from_string_non_integer_frames(frame_uri):
    """Checks *ValueError* is raised if unexpected resource delimiter found."""
    with pytest.raises(ValueError, match='non-integral frame numbers'):
        URI.from_string(frame_uri)


@pytest.mark.parametrize('service', ['', 'ftp://', 'sftp://', 'ssh://'])
def test_from_string_invalid_uri_protocol(service):
    """Checks *ValueError* raised when the URI string is invalid."""
    with pytest.raises(ValueError, match=r'Only HTTP\[S\] URLs'):
        URI.from_string(f'{service}invalid_url')


def test_from_string_unsupported_suffix():
    """Checks *ValueError* raised when suffix is incompatible with URI type."""
    # Note that if any of the metadata, rendered, or thumbnail suffixes is
    # supplied with only the base URL, the entire URI (including the
    # perceived suffix) shall be treated as the base URL.
    with pytest.raises(ValueError, match='lalala'):
        URI.from_string(f'{_FRAME_URI}/metadata')


@pytest.mark.parametrize(
    'child,parent',
    [(URI.from_string(_BASE_URL), URI.from_string(_BASE_URL)),
     (URI.from_string(_STUDY_URI), URI.from_string(_BASE_URL)),
     (URI.from_string(_SERIES_URI), URI.from_string(_STUDY_URI)),
     (URI.from_string(_INSTANCE_URI), URI.from_string(_SERIES_URI)),
     (URI.from_string(_FRAME_URI), URI.from_string(_INSTANCE_URI)),
     (URI.from_string(f'{_STUDY_URI}/{URISuffix.RENDERED.value}'),
      URI.from_string(_STUDY_URI)),
     (URI.from_string(f'{_SERIES_URI}/{URISuffix.RENDERED.value}'),
      URI.from_string(_SERIES_URI)),
     (URI.from_string(f'{_INSTANCE_URI}/{URISuffix.RENDERED.value}'),
      URI.from_string(_INSTANCE_URI)),
     (URI.from_string(f'{_FRAME_URI}/{URISuffix.RENDERED.value}'),
      URI.from_string(_FRAME_URI)),
     ])
def test_parent(child, parent):
    """Validates the expected parent URI from `parent` attribute."""
    assert str(child.parent) == str(parent)


@pytest.mark.parametrize('uri,hash_args', [
    (URI.from_string(_BASE_URL), (_BASE_URL, None, None, None, None, None)),
    (URI.from_string(_STUDY_URI),
     (_BASE_URL, _STUDY_UID, None, None, None, None)),
    (URI.from_string(_SERIES_URI),
     (_BASE_URL, _STUDY_UID, _SERIES_UID, None, None, None)),
    (URI.from_string(_INSTANCE_URI),
     (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID, None, None)),
    (URI.from_string(_FRAME_URI),
     (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID, _FRAMES, None)),
    (URI.from_string(f'{_FRAME_URI}/rendered'),
     (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID, _FRAMES,
      URISuffix.RENDERED)),
])
def test_hash(uri, hash_args):
    """Locks down the implementation of `__hash__()`."""
    assert hash(uri) == hash((*hash_args, ))


@pytest.mark.parametrize('uri,init_args', [
    (URI.from_string(_BASE_URL), (_BASE_URL, None, None, None, None, None)),
    (URI.from_string(_STUDY_URI),
     (_BASE_URL, _STUDY_UID, None, None, None, None)),
    (URI.from_string(_SERIES_URI),
     (_BASE_URL, _STUDY_UID, _SERIES_UID, None, None, None)),
    (URI.from_string(_INSTANCE_URI),
     (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID, None, None)),
    (URI.from_string(_FRAME_URI),
     (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID, _FRAMES, None)),
    (URI.from_string(f'{_FRAME_URI}/rendered'),
     (_BASE_URL, _STUDY_UID, _SERIES_UID, _INSTANCE_UID, _FRAMES,
      URISuffix.RENDERED)),
])
def test_repr(uri, init_args):
    """Locks down the implementation of `__repr__()`."""
    expected_repr = (
        'dicomweb_client.URI(base_url={}, study_instance_uid={}, '
        'series_instance_uid={}, sop_instance_uid={}, frames={}, '
        'suffix={})').format(*(repr(arg) for arg in init_args))
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


def test_eq_not_implemented():
    """Tests the `==` operator implementation for incompatible type."""
    assert URI(_BASE_URL) != 0


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
    ((_BASE_URL, '1', '2'), (None, None, None, '3', None, URISuffix.RENDERED),
     (_BASE_URL, '1', '2', '3', None, URISuffix.RENDERED)),
    ((_BASE_URL, '1', '2', '3'), ('https://new', ),
     ('https://new', '1', '2', '3')),
    ((_BASE_URL, '1', '2', '3'), (None, '4'), (_BASE_URL, '4', '2', '3')),
    ((_BASE_URL, '1', '2', '3'), (None, None, '4'),
     (_BASE_URL, '1', '4', '3')),
    ((_BASE_URL, '1', '2', '3'), (None, None, None, '4'),
     (_BASE_URL, '1', '2', '4')),
    ((_BASE_URL, '1', '2', '3', [4, 5]), (None, None, None, None, [6, 7]),
     (_BASE_URL, '1', '2', '3', [6, 7])),
])
def test_update(uri_args, update_args, expected_uri_args):
    """Tests for failure if the `URI` returned by `update()` is invalid."""
    actual_uri = URI(*uri_args).update(*update_args)
    expected_uri = URI(*expected_uri_args)
    assert actual_uri == expected_uri


@pytest.mark.parametrize('original,update,expected', [
    (None, None, False),
    (None, False, False),
    (None, True, True),
    (False, None, False),
    (True, None, True),
    (False, False, False),
    (False, True, True),
    (True, False, False),
    (True, True, True),
])
def test_update_permissive(original, update, expected):
    """Tests for the expected value of `permissive` flag in `URI.update()`."""
    if original is None:
        original_uri = URI(_BASE_URL)
    else:
        original_uri = URI(_BASE_URL, permissive=original)
    updated_uri = original_uri.update(permissive=update)
    assert updated_uri.permissive == expected


@pytest.mark.parametrize('uri_args,update_args,error_msg', [
    ((_BASE_URL, ), (None, None, '1', None, None),
     '`study_instance_uid` missing'),
    ((_BASE_URL, ), (None, None, None, '2', None),
     '`series_instance_uid` missing'),
    ((_BASE_URL, ), (None, None, '1', '2', None),
     '`study_instance_uid` missing'),
    ((_BASE_URL, _STUDY_UID), (None, None, None, '2', None),
     '`series_instance_uid` missing'),
    ((_BASE_URL, _STUDY_UID), (None, None, None, None, _FRAMES),
     '`sop_instance_uid` missing'),
])
def test_update_error(uri_args, update_args, error_msg):
    """Tests for failure if the `URI` returned by `update()` is invalid."""
    with pytest.raises(ValueError, match=error_msg):
        URI(*uri_args).update(*update_args)
