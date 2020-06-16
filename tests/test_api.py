import json
import xml.etree.ElementTree as ET
from io import BytesIO
from http import HTTPStatus

import pytest
import pydicom
from requests.exceptions import HTTPError
from retrying import RetryError

from dicomweb_client.api import (
    DICOMwebClient,
    load_json_dataset,
    _load_xml_dataset
)


def test_url(httpserver):
    protocol = 'http'
    host = 'localhost'
    port = 8080
    path = '/dcm4chee-arc/aets/DCM4CHEE/rs'
    url = '{protocol}://{host}:{port}{path}'.format(
        protocol=protocol, host=host, port=port, path=path
    )
    client = DICOMwebClient(url)
    assert client.protocol == protocol
    assert client.host == host
    assert client.port == port
    assert client.url_prefix == path
    assert client.qido_url_prefix is None
    assert client.wado_url_prefix is None
    assert client.stow_url_prefix is None


def test_url_prefixes(httpserver):
    wado_url_prefix = 'wado'
    qido_url_prefix = 'qido'
    stow_url_prefix = 'stow'
    client = DICOMwebClient(
        httpserver.url,
        wado_url_prefix=wado_url_prefix,
        qido_url_prefix=qido_url_prefix,
        stow_url_prefix=stow_url_prefix,
    )
    assert client.url_prefix == ''
    assert client.qido_url_prefix == qido_url_prefix
    assert client.wado_url_prefix == wado_url_prefix
    assert client.stow_url_prefix == stow_url_prefix


def test_proxies(httpserver):
    protocol = 'http'
    address = 'foo.com'
    proxies = {protocol: address}
    client = DICOMwebClient(httpserver.url, proxies=proxies)
    assert client._session.proxies[protocol] == address


def test_headers(httpserver):
    name = 'my-token'
    value = 'topsecret'
    headers = {name: value}
    client = DICOMwebClient(httpserver.url, headers=headers)
    client.store_instances([])
    request = httpserver.requests[0]
    assert request.headers[name] == value


def test_lookup_tag(httpserver, client):
    assert client.lookup_tag('StudyInstanceUID') == '0020000D'
    assert client.lookup_tag('SeriesInstanceUID') == '0020000E'
    assert client.lookup_tag('SOPInstanceUID') == '00080018'
    assert client.lookup_tag('PixelData') == '7FE00010'


def test_lookup_keyword(httpserver, client):
    assert client.lookup_keyword('0020000D') == 'StudyInstanceUID'
    assert client.lookup_keyword('0020000E') == 'SeriesInstanceUID'
    assert client.lookup_keyword('00080018') == 'SOPInstanceUID'
    assert client.lookup_keyword('7FE00010') == 'PixelData'


def test_set_http_retry_params(httpserver, client):
    retry = True
    retriable_error_codes = (HTTPStatus.TOO_MANY_REQUESTS,
                             HTTPStatus.SERVICE_UNAVAILABLE)
    max_attempts = 10
    wait_exponential_multiplier = 100
    client = DICOMwebClient(httpserver.url)
    client.set_http_retry_params(retry, max_attempts,
                                 wait_exponential_multiplier,
                                 retriable_error_codes)
    assert client._http_retry == retry
    assert client._http_retrable_errors == retriable_error_codes
    assert client._max_attempts == max_attempts
    assert client._wait_exponential_multiplier == wait_exponential_multiplier


def test_search_for_studies(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('search_for_studies.json'))
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    assert client.search_for_studies() == parsed_content
    request = httpserver.requests[0]
    assert request.path == '/studies'
    assert all(
        mime[0] in ('application/json', 'application/dicom+json')
        for mime in request.accept_mimetypes
    )


def test_search_for_studies_with_retries(httpserver, client, cache_dir):
    headers = {'content-type': 'application/dicom+json'}
    max_attempts = 3
    client.set_http_retry_params(
        retry=True,
        max_attempts=max_attempts,
        wait_exponential_multiplier=10
    )
    httpserver.serve_content(
        content='',
        code=HTTPStatus.REQUEST_TIMEOUT,
        headers=headers
    )
    with pytest.raises(RetryError):
        client.search_for_studies()
    assert len(httpserver.requests) == max_attempts


def test_search_for_studies_with_no_retries(httpserver, client, cache_dir):
    client.set_http_retry_params(retry=False)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(
        content='',
        code=HTTPStatus.REQUEST_TIMEOUT,
        headers=headers
    )
    with pytest.raises(HTTPError):
        client.search_for_studies()
    assert len(httpserver.requests) == 1


def test_search_for_studies_qido_prefix(httpserver, client, cache_dir):
    client.qido_url_prefix = 'qidors'
    cache_filename = str(cache_dir.joinpath('search_for_studies.json'))
    with open(cache_filename, 'r') as f:
        content = f.read()
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    client.search_for_studies()
    request = httpserver.requests[0]
    assert request.path == '/qidors/studies'


def test_search_for_studies_limit_offset(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('search_for_studies.json'))
    with open(cache_filename, 'r') as f:
        data = json.loads(f.read())
    # We will limit the search to 2 studies starting with the 2nd.
    content = json.dumps(data[1:3])
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    assert client.search_for_studies(limit=2, offset=1) == parsed_content
    request = httpserver.requests[0]
    assert (
        request.query_string.decode() == 'limit=2&offset=1' or
        request.query_string.decode() == 'offset=1&limit=2'
    )
    assert request.path == '/studies'
    assert all(
        mime[0] in ('application/json', 'application/dicom+json')
        for mime in request.accept_mimetypes
    )


def test_search_for_series(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('search_for_series.json'))
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    assert client.search_for_series() == parsed_content
    request = httpserver.requests[0]
    assert request.path == '/series'
    assert all(
        mime[0] in ('application/json', 'application/dicom+json')
        for mime in request.accept_mimetypes
    )


def test_search_for_series_wrong_uid_type(httpserver, client, cache_dir):
    study_instance_uid = ['1.2.3.4']
    with pytest.raises(TypeError):
        client.search_for_series(study_instance_uid=study_instance_uid)


def test_search_for_series_wrong_uid_value(httpserver, client, cache_dir):
    study_instance_uid = '1_2_3_4'
    with pytest.raises(ValueError):
        client.search_for_series(study_instance_uid=study_instance_uid)


def test_search_for_series_limit_offset(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('search_for_series.json'))
    with open(cache_filename, 'r') as f:
        data = json.loads(f.read())
    content = json.dumps(data[1:3])
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    assert client.search_for_studies(limit=2, offset=1) == parsed_content
    request = httpserver.requests[0]
    assert (
        request.query_string.decode() == 'limit=2&offset=1' or
        request.query_string.decode() == 'offset=1&limit=2'
    )
    assert request.path == '/studies'
    assert all(
        mime[0] in ('application/json', 'application/dicom+json')
        for mime in request.accept_mimetypes
    )


def test_search_for_instances(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('search_for_instances.json'))
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    assert client.search_for_instances() == parsed_content
    request = httpserver.requests[0]
    assert request.path == '/instances'
    assert all(
        mime[0] in ('application/json', 'application/dicom+json')
        for mime in request.accept_mimetypes
    )


def test_search_for_instances_limit_offset(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('search_for_instances.json'))
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    assert client.search_for_instances(limit=2, offset=1) == parsed_content
    request = httpserver.requests[0]
    assert (
        request.query_string.decode() == 'limit=2&offset=1' or
        request.query_string.decode() == 'offset=1&limit=2'
    )
    assert request.path == '/instances'
    assert all(
        mime[0] in ('application/json', 'application/dicom+json')
        for mime in request.accept_mimetypes
    )


def test_search_for_instances_includefields(httpserver, client, cache_dir):
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content='', code=200, headers=headers)
    f1 = 'StudyInstanceUID'
    f2 = 'SeriesInstanceUID'
    client.search_for_instances(fields={f1, f2})
    request = httpserver.requests[0]
    query_string_opt_1 = 'includefield={}&includefield={}'.format(f1, f2)
    query_string_opt_2 = 'includefield={}&includefield={}'.format(f2, f1)
    assert (
        request.query_string.decode() == query_string_opt_1 or
        request.query_string.decode() == query_string_opt_2
    )
    assert request.path == '/instances'
    assert all(
        mime[0] in ('application/json', 'application/dicom+json')
        for mime in request.accept_mimetypes
    )


def test_retrieve_instance_metadata(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('retrieve_instance_metadata.json'))
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    result = client.retrieve_instance_metadata(
        study_instance_uid, series_instance_uid, sop_instance_uid
    )
    assert result == parsed_content[0]
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}/instances'
        '/{sop_instance_uid}/metadata'.format(**locals())
    )
    assert request.path == expected_path
    assert all(
        mime[0] in ('application/json', 'application/dicom+json')
        for mime in request.accept_mimetypes
    )


def test_retrieve_instance_metadata_wado_prefix(httpserver, client, cache_dir):
    client.wado_url_prefix = 'wadors'
    cache_filename = str(cache_dir.joinpath('retrieve_instance_metadata.json'))
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    client.retrieve_instance_metadata(
        study_instance_uid, series_instance_uid, sop_instance_uid
    )
    request = httpserver.requests[0]
    expected_path = (
        '/wadors/studies/{study_instance_uid}'
        '/series/{series_instance_uid}'
        '/instances/{sop_instance_uid}/metadata'.format(**locals())
    )
    assert request.path == expected_path


def test_retrieve_series(client, httpserver, cache_dir):
    cache_filename = str(cache_dir.joinpath('file.dcm'))
    with open(cache_filename, 'rb') as f:
        payload = f.read()
    content = b''
    for i in range(3):
        content += b'\r\n--boundary\r\n'
        content += b'Content-Type: application/dicom\r\n\r\n'
        content += payload
    content += b'\r\n--boundary--'
    headers = {
        'content-type': (
            'multipart/related; '
            'type="application/dicom"; '
            'boundary="boundary" '
        ),
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    results = client.retrieve_series(
        study_instance_uid, series_instance_uid
    )
    results = list(results)
    assert len(results) == 3
    for result in results:
        with BytesIO() as fp:
            pydicom.dcmwrite(fp, result)
            raw_result = fp.getvalue()
        assert raw_result == payload
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}'
        .format(**locals())
    )
    assert request.path == expected_path
    assert request.accept_mimetypes[0][0][:43] == headers['content-type'][:43]


def test_retrieve_instance(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('file.dcm'))
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type': (
            'multipart/related; '
            'type="application/dicom"'
        ),
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    result = client.retrieve_instance(
        study_instance_uid, series_instance_uid, sop_instance_uid
    )
    with BytesIO() as fp:
        pydicom.dcmwrite(fp, result)
        raw_result = fp.getvalue()
    assert raw_result == content
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}/instances'
        '/{sop_instance_uid}'.format(**locals())
    )
    assert request.path == expected_path
    assert request.accept_mimetypes[0][0][:43] == headers['content-type'][:43]


def test_retrieve_instance_any_transfer_syntax(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('file.dcm'))
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type': 'multipart/related; type="application/dicom"',
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    client.retrieve_instance(
        study_instance_uid, series_instance_uid, sop_instance_uid,
        media_types=(
            ('application/dicom', '*', ),
        )
    )
    request = httpserver.requests[0]
    assert request.accept_mimetypes[0][0][:43] == headers['content-type'][:43]


def test_retrieve_instance_default_transfer_syntax(httpserver, client,
                                                   cache_dir):
    cache_filename = str(cache_dir.joinpath('file.dcm'))
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type': 'multipart/related; type="application/dicom"',
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    client.retrieve_instance(
        study_instance_uid, series_instance_uid, sop_instance_uid,
        media_types=(
            ('application/dicom', '1.2.840.10008.1.2.1', ),
        )
    )
    request = httpserver.requests[0]
    assert request.accept_mimetypes[0][0][:43] == headers['content-type'][:43]


def test_retrieve_instance_wrong_transfer_syntax(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('file.dcm'))
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type': 'multipart/related; type="application/dicom"',
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    with pytest.raises(ValueError):
        client.retrieve_instance(
            study_instance_uid, series_instance_uid, sop_instance_uid,
            media_types=(
                ('application/dicom', '1.2.3', ),
            )
        )


def test_retrieve_instance_wrong_mime_type(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('file.dcm'))
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type': 'multipart/related; type="image/dicom"',
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    with pytest.raises(ValueError):
        client.retrieve_instance(
            study_instance_uid, series_instance_uid, sop_instance_uid,
            media_types=(
                ('image/dicom', '1.2.840.10008.1.2.1', ),
            )
        )


def test_retrieve_instance_frames_jpeg(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('retrieve_instance_pixeldata.jpg'))
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type': 'multipart/related; type="image/jpeg"',
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    frame_numbers = [114]
    frame_list = ','.join([str(n) for n in frame_numbers])
    result = client.retrieve_instance_frames(
        study_instance_uid, series_instance_uid, sop_instance_uid,
        frame_numbers, media_types=('image/jpeg', )
    )
    assert list(result) == [content]
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}/instances'
        '/{sop_instance_uid}/frames/{frame_list}'.format(**locals())
    )
    assert request.path == expected_path
    assert request.accept_mimetypes[0][0][:36] == headers['content-type'][:36]


def test_retrieve_instance_frames_jpeg_default_transfer_syntax(httpserver,
                                                               client,
                                                               cache_dir):
    cache_filename = str(cache_dir.joinpath('retrieve_instance_pixeldata.jpg'))
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type': 'multipart/related; type="image/jpeg"',
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    frame_numbers = [114]
    client.retrieve_instance_frames(
        study_instance_uid, series_instance_uid, sop_instance_uid,
        frame_numbers, media_types=(
            ('image/jpeg', '1.2.840.10008.1.2.4.50', ),
        )
    )
    request = httpserver.requests[0]
    assert request.accept_mimetypes[0][0][:36] == headers['content-type'][:36]


def test_retrieve_instance_frames_jp2(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('retrieve_instance_pixeldata.jp2'))
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type': 'multipart/related; type="image/jp2"',
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    frame_numbers = [114]
    frame_list = ','.join([str(n) for n in frame_numbers])
    result = client.retrieve_instance_frames(
        study_instance_uid, series_instance_uid, sop_instance_uid,
        frame_numbers, media_types=('image/jp2', )
    )
    assert list(result) == [content]
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}/instances'
        '/{sop_instance_uid}/frames/{frame_list}'.format(**locals())
    )
    assert request.path == expected_path
    assert request.accept_mimetypes[0][0][:35] == headers['content-type'][:35]


def test_retrieve_instance_frames_rendered_jpeg(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('retrieve_instance_pixeldata.jpg'))
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type': 'image/jpeg',
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    frame_numbers = [1]
    result = client.retrieve_instance_frames_rendered(
        study_instance_uid, series_instance_uid, sop_instance_uid,
        frame_numbers, media_types=('image/jpeg', )
    )
    assert result == content
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}/instances'
        '/{sop_instance_uid}/frames/{frame_numbers}/rendered'.format(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid,
            frame_numbers=','.join([str(n) for n in frame_numbers])
        )
    )
    assert request.path == expected_path
    assert request.accept_mimetypes[0][0][:11] == headers['content-type'][:11]


def test_retrieve_instance_frames_rendered_jpeg_transfer_syntax(httpserver,
                                                                client):
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    frame_numbers = [1]
    with pytest.raises(TypeError):
        client.retrieve_instance_frames_rendered(
            study_instance_uid, series_instance_uid, sop_instance_uid,
            frame_numbers, media_types=(
                ('image/jpeg', '1.2.840.10008.1.2.4.50', ),
            )
        )


def test_retrieve_instance_frames_rendered_png(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('retrieve_instance_pixeldata.png'))
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type': 'image/png',
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    frame_numbers = [1]
    result = client.retrieve_instance_frames_rendered(
        study_instance_uid, series_instance_uid, sop_instance_uid,
        frame_numbers, media_types=('image/png', )
    )
    assert result == content
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}/instances'
        '/{sop_instance_uid}/frames/{frame_numbers}/rendered'.format(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid,
            frame_numbers=','.join([str(n) for n in frame_numbers])
        )
    )
    assert request.path == expected_path
    assert request.accept_mimetypes[0][0][:10] == headers['content-type'][:10]


def test_store_instance_error_with_retries(httpserver, client, cache_dir):
    dataset = load_json_dataset({})
    dataset.is_little_endian = True
    dataset.is_implicit_VR = True
    max_attempts = 2
    client.set_http_retry_params(
        retry=True,
        max_attempts=max_attempts,
        wait_exponential_multiplier=10
    )
    httpserver.serve_content(
        content='',
        code=HTTPStatus.REQUEST_TIMEOUT,
        headers=''
    )
    with pytest.raises(RetryError):
        client.store_instances([dataset])
    assert len(httpserver.requests) == max_attempts
    request = httpserver.requests[0]
    assert request.headers['Content-Type'].startswith(
        'multipart/related; type="application/dicom"'
    )


def test_store_instance_error_with_no_retries(httpserver, client, cache_dir):
    dataset = load_json_dataset({})
    dataset.is_little_endian = True
    dataset.is_implicit_VR = True
    client.set_http_retry_params(retry=False)
    httpserver.serve_content(
        content='',
        code=HTTPStatus.REQUEST_TIMEOUT,
        headers=''
    )
    with pytest.raises(HTTPError):
        client.store_instances([dataset])
    assert len(httpserver.requests) == 1
    request = httpserver.requests[0]
    assert request.headers['Content-Type'].startswith(
        'multipart/related; type="application/dicom"'
    )


def test_delete_study_error(httpserver, client, cache_dir):
    study_instance_uid = '1.2.3'
    httpserver.serve_content(
        content='',
        code=HTTPStatus.METHOD_NOT_ALLOWED,
        headers=''
    )
    with pytest.raises(HTTPError):
        client.delete_study(study_instance_uid=study_instance_uid)
    assert len(httpserver.requests) == 1
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}'.format(
            study_instance_uid=study_instance_uid)
    )
    assert request.path == expected_path
    assert request.method == 'DELETE'


def test_delete_series_error(httpserver, client, cache_dir):
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    httpserver.serve_content(
        content='',
        code=HTTPStatus.METHOD_NOT_ALLOWED,
        headers=''
    )
    with pytest.raises(HTTPError):
        client.delete_series(study_instance_uid=study_instance_uid,
                             series_instance_uid=series_instance_uid)
    assert len(httpserver.requests) == 1
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}'.format(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid)
    )
    assert request.path == expected_path
    assert request.method == 'DELETE'


def test_delete_instance_error(httpserver, client, cache_dir):
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    httpserver.serve_content(
        content='',
        code=HTTPStatus.METHOD_NOT_ALLOWED,
        headers=''
    )
    with pytest.raises(HTTPError):
        client.delete_instance(study_instance_uid=study_instance_uid,
                               series_instance_uid=series_instance_uid,
                               sop_instance_uid=sop_instance_uid)
    assert len(httpserver.requests) == 1
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}/instances'
        '/{sop_instance_uid}'.format(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid,)
    )
    assert request.path == expected_path
    assert request.method == 'DELETE'


def test_load_json_dataset_da(httpserver, client, cache_dir):
    value = ['2018-11-21']
    dicom_json = {
        '00080020': {
            'vr': 'DA',
            'Value': value
        }
    }
    dataset = load_json_dataset(dicom_json)
    assert dataset.StudyDate == value[0]


def test_load_json_dataset_tm(httpserver, client, cache_dir):
    value = ['113924']
    dicom_json = {
        '00080030': {
            'vr': 'TM',
            'Value': value,
        },
    }
    dataset = load_json_dataset(dicom_json)
    assert dataset.StudyTime == value[0]


def test_load_json_dataset_pn_vm1(httpserver, client, cache_dir):
    name = 'Only^Person'
    value = [{'Alphabetic': name}]
    dicom_json = {
        '00080090': {
            'vr': 'PN',
            'Value': value,
        },
    }
    dataset = load_json_dataset(dicom_json)
    assert dataset.ReferringPhysicianName == name


def test_load_json_dataset_pn_vm2(httpserver, client, cache_dir):
    names = ['First^Person', 'Second^Person']
    value = [{'Alphabetic': names[0]}, {'Alphabetic': names[1]}]
    dicom_json = {
        '0008009C': {
            'vr': 'PN',
            'Value': value,
        },
    }
    dataset = load_json_dataset(dicom_json)
    assert dataset.ConsultingPhysicianName == names


def test_load_json_dataset_pn_vm1_empty(httpserver, client, cache_dir):
    value = [{}]
    dicom_json = {
        '00080090': {
            'vr': 'PN',
            'Value': value,
        },
    }
    dataset = load_json_dataset(dicom_json)
    # This returns different results for Python2 (None) and Python3 ("")
    assert dataset.ReferringPhysicianName in (None, '')


def test_load_json_dataset_pn_vm2_empty(httpserver, client, cache_dir):
    value = [{}]
    dicom_json = {
        '0008009C': {
            'vr': 'PN',
            'Value': value,
        },
    }
    dataset = load_json_dataset(dicom_json)
    assert dataset.ConsultingPhysicianName == []


def test_load_xml_response(httpserver, client, cache_dir):
    cache_filename = str(cache_dir.joinpath('store.xml'))
    with open(cache_filename, 'rb') as f:
        tree = ET.fromstring(f.read())
        dataset = _load_xml_dataset(tree)
    assert dataset.RetrieveURL.startswith('https://wadors.hospital.com')
    assert len(dataset.ReferencedSOPSequence) == 2
