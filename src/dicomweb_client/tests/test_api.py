import os
import json
from io import BytesIO

import pytest
import pydicom


def test_search_for_studies(httpserver, client, cache_dir):
    cache_filename = os.path.join(cache_dir, 'search_for_studies.json')
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    assert client.search_for_studies() == parsed_content
    request = httpserver.requests[0]
    assert request.path == '/studies'
    assert request.accept_mimetypes[0][0] == 'application/dicom+json'


def test_search_for_studies_qido_prefix(httpserver, client, cache_dir):
    client.qido_url_prefix = 'qidors'
    cache_filename = os.path.join(cache_dir, 'search_for_studies.json')
    with open(cache_filename, 'r') as f:
        content = f.read()
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    client.search_for_studies()
    request = httpserver.requests[0]
    assert request.path == '/qidors/studies'


def test_search_for_studies_limit_offset(httpserver, client, cache_dir):
    cache_filename = os.path.join(cache_dir, 'search_for_studies.json')
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
    assert request.accept_mimetypes[0][0] == 'application/dicom+json'


def test_search_for_series(httpserver, client, cache_dir):
    cache_filename = os.path.join(cache_dir, 'search_for_series.json')
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    assert client.search_for_series() == parsed_content
    request = httpserver.requests[0]
    assert request.path == '/series'
    assert request.accept_mimetypes[0][0] == 'application/dicom+json'


def test_search_for_series_wrong_uid_type(httpserver, client, cache_dir):
    study_instance_uid = ['1.2.3.4']
    with pytest.raises(TypeError):
        client.search_for_series(study_instance_uid=study_instance_uid)


def test_search_for_series_wrong_uid_value(httpserver, client, cache_dir):
    study_instance_uid = '1_2_3_4'
    with pytest.raises(ValueError):
        client.search_for_series(study_instance_uid=study_instance_uid)


def test_search_for_series_limit_offset(httpserver, client, cache_dir):
    cache_filename = os.path.join(cache_dir, 'search_for_series.json')
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
    assert request.accept_mimetypes[0][0] == 'application/dicom+json'


def test_search_for_instances(httpserver, client, cache_dir):
    cache_filename = os.path.join(cache_dir, 'search_for_instances.json')
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    assert client.search_for_instances() == parsed_content
    request = httpserver.requests[0]
    assert request.path == '/instances'
    assert request.accept_mimetypes[0][0] == 'application/dicom+json'


def test_search_for_instances_limit_offset(httpserver, client, cache_dir):
    cache_filename = os.path.join(cache_dir, 'search_for_instances.json')
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
    assert request.accept_mimetypes[0][0] == 'application/dicom+json'


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
    assert request.accept_mimetypes[0][0] == 'application/dicom+json'


def test_retrieve_instance_metadata(httpserver, client, cache_dir):
    cache_filename = os.path.join(cache_dir, 'retrieve_instance_metadata.json')
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
    assert result == parsed_content
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}/instances'
        '/{sop_instance_uid}/metadata'.format(**locals())
    )
    assert request.path == expected_path
    assert request.accept_mimetypes[0][0] == 'application/dicom+json'


def test_retrieve_instance_metadata_wado_prefix(httpserver, client, cache_dir):
    client.wado_url_prefix = 'wadors'
    cache_filename = os.path.join(cache_dir, 'retrieve_instance_metadata.json')
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
    request = httpserver.requests[0]
    expected_path = (
        '/wadors/studies/{study_instance_uid}'
        '/series/{series_instance_uid}'
        '/instances/{sop_instance_uid}/metadata'.format(**locals())
    )
    assert request.path == expected_path


def test_retrieve_instance(httpserver, client, cache_dir):
    cache_filename = os.path.join(cache_dir, 'file.dcm')
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type':
            'multipart/related; type="application/dicom"; boundary="boundary"',
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
    assert request.accept_mimetypes[0][0][:45] == headers['content-type'][:45]


def test_retrieve_instance_pixeldata_jpeg(httpserver, client, cache_dir):
    cache_filename = os.path.join(cache_dir, 'retrieve_instance_pixeldata.jpg')
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type':
            'multipart/related; type="image/jpeg"; boundary="boundary"'
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    frame_numbers = [114]
    frame_list = ','.join([str(n) for n in frame_numbers])
    result = client.retrieve_instance_frames(
        study_instance_uid, series_instance_uid, sop_instance_uid,
        frame_numbers, image_format='jpeg'
    )
    assert result == [content]
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}/instances'
        '/{sop_instance_uid}/frames/{frame_list}'.format(**locals())
    )
    assert request.path == expected_path
    assert request.accept_mimetypes[0][0][:38] == headers['content-type'][:38]


def test_retrieve_instance_pixeldata_jp2(httpserver, client, cache_dir):
    cache_filename = os.path.join(cache_dir, 'retrieve_instance_pixeldata.jp2')
    with open(cache_filename, 'rb') as f:
        content = f.read()
    headers = {
        'content-type':
            'multipart/related; type="image/jp2"; boundary="boundary"'
    }
    httpserver.serve_content(content=content, code=200, headers=headers)
    study_instance_uid = '1.2.3'
    series_instance_uid = '1.2.4'
    sop_instance_uid = '1.2.5'
    frame_numbers = [114]
    frame_list = ','.join([str(n) for n in frame_numbers])
    result = client.retrieve_instance_frames(
        study_instance_uid, series_instance_uid, sop_instance_uid,
        frame_numbers, image_format='jp2'
    )
    assert result == [content]
    request = httpserver.requests[0]
    expected_path = (
        '/studies/{study_instance_uid}/series/{series_instance_uid}/instances'
        '/{sop_instance_uid}/frames/{frame_list}'.format(**locals())
    )
    assert request.path == expected_path
    assert request.accept_mimetypes[0][0][:37] == headers['content-type'][:37]
