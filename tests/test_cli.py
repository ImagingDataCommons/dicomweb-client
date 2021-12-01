import json
import tempfile

import pytest
import pydicom

from dicomweb_client.cli import main


def test_parse_search_studies(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'search', 'studies'
    ])
    assert getattr(args, 'method') == 'search'
    assert getattr(args, 'qido_ie') == 'studies'
    assert getattr(args, 'prettify') is False
    assert getattr(args, 'dicomize') is False
    with pytest.raises(AttributeError):
        getattr(args, 'study_instance_uid')
    with pytest.raises(AttributeError):
        getattr(args, 'series_instance_uid')
    with pytest.raises(AttributeError):
        getattr(args, 'sop_instance_uid')


def test_parse_search_studies_series(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002',
            'search', 'studies', '--series', '1.2.3'
        ])


def test_parse_search_studies_instance(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'search', 'studies',
            '--instance', '1.2.3'
        ])


def test_parse_search_series(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'search', 'series'
    ])
    assert getattr(args, 'method') == 'search'
    assert getattr(args, 'qido_ie') == 'series'
    assert getattr(args, 'prettify') is False
    assert getattr(args, 'dicomize') is False
    assert getattr(args, 'study_instance_uid') is None
    with pytest.raises(AttributeError):
        getattr(args, 'series_instance_uid')
    with pytest.raises(AttributeError):
        getattr(args, 'sop_instance_uid')


def test_parse_search_series_specific_study(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'search', 'series',
        '--study', '1.2.3'
    ])
    assert getattr(args, 'method') == 'search'
    assert getattr(args, 'qido_ie') == 'series'
    assert getattr(args, 'prettify') is False
    assert getattr(args, 'dicomize') is False
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    with pytest.raises(AttributeError):
        getattr(args, 'series_instance_uid')
    with pytest.raises(AttributeError):
        getattr(args, 'sop_instance_uid')


def test_parse_search_series_wrong_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'search', 'series',
            '--series', '1.2.3'
        ])


def test_parse_search_instances(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'search', 'instances'
    ])
    assert getattr(args, 'method') == 'search'
    assert getattr(args, 'qido_ie') == 'instances'
    assert getattr(args, 'prettify') is False
    assert getattr(args, 'dicomize') is False
    assert getattr(args, 'study_instance_uid') is None
    assert getattr(args, 'series_instance_uid') is None
    with pytest.raises(AttributeError):
        getattr(args, 'sop_instance_uid')


def test_parse_search_instances_specific_study(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'search', 'instances',
        '--study', '1.2.3'
    ])
    assert getattr(args, 'method') == 'search'
    assert getattr(args, 'qido_ie') == 'instances'
    assert getattr(args, 'prettify') is False
    assert getattr(args, 'dicomize') is False
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'series_instance_uid') is None
    with pytest.raises(AttributeError):
        getattr(args, 'sop_instance_uid')


def test_parse_search_instances_specific_study_series(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'search', 'instances',
        '--study', '1.2.3', '--series', '1.2.4'
    ])
    assert getattr(args, 'method') == 'search'
    assert getattr(args, 'qido_ie') == 'instances'
    assert getattr(args, 'prettify') is False
    assert getattr(args, 'dicomize') is False
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'series_instance_uid') == '1.2.4'
    with pytest.raises(AttributeError):
        getattr(args, 'sop_instance_uid')


def test_parse_search_instances_prettify(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'search', 'instances', '--prettify'
    ])
    assert getattr(args, 'method') == 'search'
    assert getattr(args, 'qido_ie') == 'instances'
    assert getattr(args, 'prettify') is True
    assert getattr(args, 'dicomize') is False


def test_parse_search_instances_dicomize(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'search', 'instances', '--dicomize'
    ])
    assert getattr(args, 'method') == 'search'
    assert getattr(args, 'qido_ie') == 'instances'
    assert getattr(args, 'prettify') is False
    assert getattr(args, 'dicomize') is True


def test_parse_search_instances_argument_conflict(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'search', 'instances',
            '--prettify', '--dicomize'
        ])


def test_parse_retrieve_study(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'studies',
        '--study', '1.2.3', 'full'
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'studies'
    assert getattr(args, 'studies_resource') == 'full'
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'save') is False
    with pytest.raises(AttributeError):
        getattr(args, 'prettify')
    with pytest.raises(AttributeError):
        getattr(args, 'dicomize')


def test_parse_retrieve_study_save(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'studies',
        '--study', '1.2.3', 'full', '--save'
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'studies'
    assert getattr(args, 'studies_resource') == 'full'
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'save') is True
    assert getattr(args, 'output_dir') == tempfile.gettempdir()


def test_parse_retrieve_study_metadata(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'studies',
        '--study', '1.2.3', 'metadata'
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'studies'
    assert getattr(args, 'studies_resource') == 'metadata'
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'prettify') is False
    assert getattr(args, 'dicomize') is False
    with pytest.raises(AttributeError):
        getattr(args, 'series_instance_uid')
    with pytest.raises(AttributeError):
        getattr(args, 'sop_instance_uid')


def test_parse_retrieve_study_metadata_unsupported_argument_media_type(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'studies',
            '--study', '1.2.3', 'metadata', '--media-type', 'application/dicom'
        ])


def test_parse_retrieve_study_metadata_missing_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'studies', 'metadata'
        ])


def test_parse_retrieve_study_metadata_wrong_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'studies',
            '--series', '1.2.3', 'metadata'
        ])


def test_parse_retrieve_series(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'series',
        '--study', '1.2.3', '--series', '1.2.4', 'full'
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'series'
    assert getattr(args, 'series_resource') == 'full'
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'series_instance_uid') == '1.2.4'
    assert getattr(args, 'save') is False
    with pytest.raises(AttributeError):
        getattr(args, 'prettify')
    with pytest.raises(AttributeError):
        getattr(args, 'dicomize')


def test_parse_retrieve_series_save(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'series',
        '--study', '1.2.3', '--series', '1.2.4', 'full', '--save'
    ])
    assert getattr(args, 'save') is True
    assert getattr(args, 'output_dir') == tempfile.gettempdir()


def test_parse_retrieve_series_save_directory(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'series',
        '--study', '1.2.3', '--series', '1.2.4', 'full', '--save',
        '--output-dir', '/path/to/dir'
    ])
    assert getattr(args, 'save') is True
    assert getattr(args, 'output_dir') == '/path/to/dir'


def test_parse_retrieve_series_metadata(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'series',
        '--study', '1.2.3', '--series', '1.2.4', 'metadata'
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'series'
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'series_instance_uid') == '1.2.4'
    assert getattr(args, 'prettify') is False
    assert getattr(args, 'dicomize') is False
    with pytest.raises(AttributeError):
        getattr(args, 'sop_instance_uid')


def test_parse_retrieve_series_metadata_extra_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'series',
            '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5',
            'metadata'
        ])


def test_parse_retrieve_series_metadata_missing_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'series',
            '--study', '1.2.3', 'metadata',
        ])


def test_parse_store_instances_single_file(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'store', 'instances',
        '/path/to/file.dcm',
    ])
    assert getattr(args, 'method') == 'store'
    assert getattr(args, 'stow_ie') == 'instances'
    assert getattr(args, 'study_instance_uid') is None
    assert getattr(args, 'files') == ['/path/to/file.dcm']


def test_parse_store_instances_chunked(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002',
        '--chunk-size', '1000',
        'store', 'instances',
        '/path/to/file.dcm',
    ])
    assert getattr(args, 'chunk_size') == 1000


def test_parse_store_instances_single_file_study_instance_uid(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'store', 'instances',
        '/path/to/file.dcm', '--study', '1.2.3',
    ])
    assert getattr(args, 'method') == 'store'
    assert getattr(args, 'stow_ie') == 'instances'
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'files') == ['/path/to/file.dcm']


def test_parse_store_instances_multiple_files(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'store', 'instances',
        '/path/to/f1.dcm', '/path/to/f2.dcm',
    ])
    assert getattr(args, 'method') == 'store'
    assert getattr(args, 'stow_ie') == 'instances'
    assert getattr(args, 'study_instance_uid') is None
    assert getattr(args, 'files') == ['/path/to/f1.dcm', '/path/to/f2.dcm']


def test_parse_store_studies(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'store', 'studies',
            '/path/to/file.dcm',
        ])


def test_parse_store_series(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'store', 'series',
            '/path/to/file.dcm',
        ])


def test_parse_retrieve_instance(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5', 'full',
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'instances'
    assert getattr(args, 'instances_resource') == 'full'
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'series_instance_uid') == '1.2.4'
    assert getattr(args, 'sop_instance_uid') == '1.2.5'
    assert getattr(args, 'save') is False
    with pytest.raises(AttributeError):
        getattr(args, 'prettify')
    with pytest.raises(AttributeError):
        getattr(args, 'dicomize')


def test_parse_retrieve_instance_chunked(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002',
        '--chunk-size', '1000',
        'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5', 'full',
    ])
    assert getattr(args, 'chunk_size') == 1000


def test_parse_retrieve_instance_media_types(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5', 'full',
        '--media-type', 'application/dicom',
    ])
    assert getattr(args, 'media_types') == [
        ['application/dicom', ],
    ]


def test_parse_retrieve_instance_media_types_transfer_syntax(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5', 'full',
        '--media-type', 'application/dicom', '1.2.840.10008.1.2.1',
    ])
    assert getattr(args, 'media_types') == [
        ['application/dicom', '1.2.840.10008.1.2.1', ],
    ]


def test_parse_retrieve_instance_media_types_transfer_syntax_multiple(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5', 'full',
        '--media-type', 'application/dicom', '1.2.840.10008.1.2.1',
        '--media-type', 'application/dicom', '1.2.840.10008.1.2.4.90',
    ])
    assert getattr(args, 'media_types') == [
        ['application/dicom', '1.2.840.10008.1.2.1', ],
        ['application/dicom', '1.2.840.10008.1.2.4.90', ],
    ]


def test_parse_retrieve_instance_metadata(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances', '--study',
        '1.2.3', '--series', '1.2.4', '--instance', '1.2.5', 'metadata'
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'instances'
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'series_instance_uid') == '1.2.4'
    assert getattr(args, 'sop_instance_uid') == '1.2.5'
    assert getattr(args, 'save') is False
    assert getattr(args, 'output_dir') == tempfile.gettempdir()
    assert getattr(args, 'prettify') is False
    assert getattr(args, 'dicomize') is False


def test_parse_retrieve_instance_metadata_missing_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'instances',
            '--study', '1.2.3', '--series', '1.2.4', 'metadata'
        ])


def test_parse_retrieve_instance_metadata_missing_argument_2(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'instances',
            '--study', '1.2.3', '--instance', '1.2.5', 'metadata'
        ])


def test_parse_retrieve_instance_metadata_missing_argument_3(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'instances',
            '--series', '1.2.4', '--instance', '1.2.5', 'metadata'
        ])


def test_parse_retrieve_instance_frames(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5',
        'frames', '--numbers', '1',
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'instances'
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'series_instance_uid') == '1.2.4'
    assert getattr(args, 'sop_instance_uid') == '1.2.5'
    assert getattr(args, 'frame_numbers') == [1]
    assert getattr(args, 'save') is False
    assert getattr(args, 'output_dir') == tempfile.gettempdir()
    assert getattr(args, 'show') is False
    with pytest.raises(AttributeError):
        assert getattr(args, 'prettify')
    with pytest.raises(AttributeError):
        assert getattr(args, 'dicomize')


def test_parse_retrieve_instance_frames_multiple(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5',
        'frames', '--numbers', '1', '2', '3',
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'instances'
    assert getattr(args, 'study_instance_uid') == '1.2.3'
    assert getattr(args, 'series_instance_uid') == '1.2.4'
    assert getattr(args, 'sop_instance_uid') == '1.2.5'
    assert getattr(args, 'frame_numbers') == [1, 2, 3]
    assert getattr(args, 'save') is False
    assert getattr(args, 'output_dir') == tempfile.gettempdir()
    assert getattr(args, 'show') is False
    with pytest.raises(AttributeError):
        assert getattr(args, 'prettify')
    with pytest.raises(AttributeError):
        assert getattr(args, 'dicomize')


def test_parse_retrieve_instance_frames_show(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5',
        'frames', '--numbers', '1', '--show',
    ])
    assert getattr(args, 'show') is True
    assert getattr(args, 'save') is False
    assert getattr(args, 'output_dir') == tempfile.gettempdir()


def test_parse_retrieve_instance_frames_save(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5',
        'frames', '--numbers', '1', '--save',
    ])
    assert getattr(args, 'show') is False
    assert getattr(args, 'save') is True
    assert getattr(args, 'output_dir') == tempfile.gettempdir()


def test_parse_retrieve_instance_frames_show_save(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5',
        'frames', '--numbers', '1', '--save', '--show'
    ])
    assert getattr(args, 'show') is True
    assert getattr(args, 'save') is True
    assert getattr(args, 'output_dir') == tempfile.gettempdir()


def test_parse_retrieve_instance_frames_save_file(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'instances',
        '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5',
        'frames', '--numbers', '1', '--save', '--output-dir', '/tmp',
    ])
    assert getattr(args, 'show') is False
    assert getattr(args, 'save') is True
    assert getattr(args, 'output_dir') == '/tmp'


def test_parse_retrieve_instance_frames_missing_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'instances',
            '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5',
            'frames', '--numbers',
        ])


def test_parse_retrieve_study_full_missing_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'studies',
            '--study', '1.2.3'
        ])


def test_parse_retrieve_series_missing_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'series',
            '--studies', '1.2.3', '--series', '1.2.4'
        ])


def test_parse_retrieve_instance_missing_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'series',
            '--study', '1.2.3', '--series', '1.2.4', '--instance', '1.2.5'
        ])


def test_parse_retrieve_bulkdata(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'bulkdata',
        '--uri', 'http://localhost:8002/bulk/data'
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'bulkdata'
    assert getattr(args, 'media_types') is None
    assert getattr(args, 'bulkdata_uri') == 'http://localhost:8002/bulk/data'


def test_parse_retrieve_bulkdata_media_type(parser):
    args = parser.parse_args([
        '--url', 'http://localhost:8002', 'retrieve', 'bulkdata',
        '--uri', 'http://localhost:8002/bulk/data',
        '--media-type', 'image/jpeg'
    ])
    assert getattr(args, 'method') == 'retrieve'
    assert getattr(args, 'wado_ie') == 'bulkdata'
    assert getattr(args, 'media_types') == [['image/jpeg', ], ]
    assert getattr(args, 'bulkdata_uri') == 'http://localhost:8002/bulk/data'


def test_parse_retrieve_bulkdata_missing_argument(parser):
    with pytest.raises(SystemExit):
        parser.parse_args([
            '--url', 'http://localhost:8002', 'retrieve', 'bulkdata'
        ])


def test_search_for_studies(parser, httpserver, cache_dir, capsys):
    cache_filename = str(cache_dir.joinpath('search_for_studies.json'))
    with open(cache_filename, 'r') as f:
        content = f.read()
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    args = parser.parse_args([
        '--url', httpserver.url, 'search', 'studies',
    ])
    with pytest.raises(SystemExit) as exit:
        main(args)
    assert exit.value.code == 0
    stdout, stderr = capsys.readouterr()
    assert stdout == content


def test_search_for_studies_dicomize(parser, httpserver, cache_dir, capsys):
    cache_filename = str(cache_dir.joinpath('search_for_studies.json'))
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    dicomized_content = '\n\n\n'.join([
        repr(pydicom.dataset.Dataset.from_json(instance))
        for instance in parsed_content
    ])
    dicomized_content += '\n\n\n'
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    args = parser.parse_args([
        '--url', httpserver.url, 'search', 'studies', '--dicomize'
    ])
    with pytest.raises(SystemExit) as exit:
        main(args)
    assert exit.value.code == 0
    stdout, stderr = capsys.readouterr()
    assert stdout == dicomized_content


def test_search_for_studies_prettify(parser, httpserver, cache_dir, capsys):
    cache_filename = str(cache_dir.joinpath('search_for_studies.json'))
    with open(cache_filename, 'r') as f:
        content = f.read()
    parsed_content = json.loads(content)
    prettified_content = json.dumps(parsed_content, indent=4, sort_keys=True)
    prettified_content += '\n'
    headers = {'content-type': 'application/dicom+json'}
    httpserver.serve_content(content=content, code=200, headers=headers)
    args = parser.parse_args([
        '--url', httpserver.url, 'search', 'studies', '--prettify'
    ])
    with pytest.raises(SystemExit) as exit:
        main(args)
    assert exit.value.code == 0
    stdout, stderr = capsys.readouterr()
    assert stdout == prettified_content
