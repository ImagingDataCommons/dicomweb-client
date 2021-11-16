"""Command Line Interface (CLI)"""
import os
import sys
import json
import logging
import argparse
import tempfile
import traceback
import getpass
from io import BytesIO

import pydicom

from dicomweb_client.api import DICOMwebClient
from dicomweb_client.log import configure_logging
from dicomweb_client.session_utils import (
    create_session,
    create_session_from_user_pass,
    add_certs_to_session,
)

logger = logging.getLogger(__name__)


def _get_parser():
    """Builds the object for parsing command line arguments.

    Returns
    -------
    argparse.ArgumentParser

    """
    parser = argparse.ArgumentParser(
        description='Client for DICOMweb RESTful services.',
        prog='dicomweb_client'
    )
    parser.add_argument(
        '-v', '--verbosity', dest='logging_verbosity', default=0,
        action='count',
        help=(
            'logging verbosity that maps to a logging level '
            '(default: error, -v: warning, -vv: info, -vvv: debug, '
            '-vvvv: debug + traceback); '
            'all log messages are written to standard error'
        )
    )
    parser.add_argument(
        '-u', '--user', dest='username', metavar='NAME',
        help='username for authentication with the DICOMweb service'
    )
    parser.add_argument(
        '-p', '--password', dest='password', metavar='PASSWORD',
        help='password for authentication with the DICOMweb service'
    )
    parser.add_argument(
        '--ca', dest='ca_bundle', metavar='CERT-FILE',
        help='path to a CA bundle file'
    )
    parser.add_argument(
        '--cert', dest='cert', metavar='CERT-FILE',
        help='path to a client certificate file in PEM format'
    )
    parser.add_argument(
        '--bearer-token', dest='bearer_token', metavar='TOKEN',
        help='bearer token for authentication with the DICOMweb service'
    )
    parser.add_argument(
        '--url', dest='url', metavar='URL',
        help='uniform resource locator of the DICOMweb service'
    )
    parser.add_argument(
        '--chunk-size', dest='chunk_size', type=int, metavar='NUM',
        default=10**6,
        help='maximum size of a network transfer chunk in bytes'
    )

    abstract_optional_study_parser = argparse.ArgumentParser(add_help=False)
    abstract_optional_study_parser.add_argument(
        '--study', metavar='UID', dest='study_instance_uid',
        help='unique study identifer (StudyInstanceUID)'
    )

    abstract_required_study_parser = argparse.ArgumentParser(add_help=False)
    abstract_required_study_parser.add_argument(
        '--study', metavar='UID', dest='study_instance_uid', required=True,
        help='unique study identifier (StudyInstanceUID)'
    )

    abstract_optional_series_parser = argparse.ArgumentParser(add_help=False)
    abstract_optional_series_parser.add_argument(
        '--series', metavar='UID', dest='series_instance_uid',
        help='unique series identifier (SeriesInstanceUID)'
    )

    abstract_required_series_parser = argparse.ArgumentParser(add_help=False)
    abstract_required_series_parser.add_argument(
        '--series', metavar='UID', dest='series_instance_uid', required=True,
        help='unique series identifier (SeriesInstanceUID)'
    )

    abstract_optional_instance_parser = argparse.ArgumentParser(add_help=False)
    abstract_optional_instance_parser.add_argument(
        '--instance', metavar='UID', dest='sop_instance_uid',
        help='unique instance identifier (SOPInstanceUID)'
    )

    abstract_required_instance_parser = argparse.ArgumentParser(add_help=False)
    abstract_required_instance_parser.add_argument(
        '--instance', metavar='UID', dest='sop_instance_uid', required=True,
        help='unique instance identifier (SOPInstanceUID)'
    )

    abstract_search_parser = argparse.ArgumentParser(add_help=False)
    abstract_search_parser.add_argument(
        '--filter', metavar='KEY=VALUE', dest='search_filters',
        action='append', default=[],
        help='query filter criterion'
    )
    abstract_search_parser.add_argument(
        '--field', metavar='NAME', dest='search_fields', action='append',
        help='field that should be included in response'
    )
    abstract_search_parser.add_argument(
        '--limit', metavar='NUM', type=int, dest='search_limit',
        help='number of items that should be maximally retrieved'
    )
    abstract_search_parser.add_argument(
        '--offset', metavar='NUM', type=int, dest='search_offset',
        help='number of items that should be skipped'
    )
    abstract_search_parser.add_argument(
        '--fuzzy', dest='search_fuzzymatching', action='store_true',
        help='perform fuzzy matching'
    )

    abstract_retrieve_parser = argparse.ArgumentParser(add_help=False)
    abstract_retrieve_parser.add_argument(
        '--media-type', metavar='MEDIATYPE', action='append', default=None,
        nargs='+', dest='media_types',
        help=(
            'acceptable media type and the optionally the UID of a '
            'corresponding tranfer syntax separted by a whitespace'
            '(e.g., "image/jpeg" or "image/jpeg 1.2.840.10008.1.2.4.50")'
        )
    )

    abstract_fmt_parser = argparse.ArgumentParser(add_help=False)
    abstract_fmt_group = abstract_fmt_parser.add_mutually_exclusive_group()
    abstract_fmt_group.add_argument(
        '--prettify', action='store_true',
        help='pretty print JSON response message'
    )
    abstract_fmt_group.add_argument(
        '--dicomize', action='store_true',
        help='convert JSON response message to DICOM data set'
    )

    abstract_save_parser = argparse.ArgumentParser(add_help=False)
    abstract_save_parser.add_argument(
        '--save', action='store_true',
        help='whether downloaded data should be saved'
    )
    abstract_save_parser.add_argument(
        '--output-dir', metavar='PATH', dest='output_dir',
        default=tempfile.gettempdir(),
        help='path to directory where downloaded data should be saved'
    )

    abstract_load_parser = argparse.ArgumentParser(add_help=False)
    abstract_load_parser.add_argument(
        metavar='PATH', dest='files', nargs='+',
        help='paths to DICOM files that should be loaded'
    )

    subparsers = parser.add_subparsers(dest='method', help='services')
    subparsers.required = True

    # QIDO
    search_parser = subparsers.add_parser(
        'search',
        description=(
            'QIDO-RS: Query based on ID for DICOM Objects by RESTful Serices.'
        )
    )
    search_subparsers = search_parser.add_subparsers(
        dest='qido_ie', metavar='INFORMATION ENTITIES', description='', help=''
    )
    search_subparsers.required = True

    # QIDO - Studies
    search_for_studies_parser = search_subparsers.add_parser(
        'studies',
        description='Search for DICOM studies.',
        parents=[abstract_search_parser, abstract_fmt_parser]
    )
    search_for_studies_parser.set_defaults(func=_search_for_studies)

    # QUIDO - Series
    search_for_series_parser = search_subparsers.add_parser(
        'series',
        description='Search for DICOM series.',
        parents=[
            abstract_search_parser, abstract_fmt_parser,
            abstract_optional_study_parser
        ]
    )
    search_for_series_parser.set_defaults(func=_search_for_series)

    # QIDO - Instances
    search_for_instances_parser = search_subparsers.add_parser(
        'instances', description='Search for DICOM instances.',
        parents=[
            abstract_fmt_parser, abstract_search_parser,
            abstract_optional_study_parser, abstract_optional_series_parser
        ]
    )
    search_for_instances_parser.set_defaults(func=_search_for_instances)

    # WADO
    retrieve_parser = subparsers.add_parser(
        'retrieve',
        description='WADO-RS: Web Access to DICOM Objects by RESTful Services.',
    )
    retrieve_subparsers = retrieve_parser.add_subparsers(
        dest='wado_ie', metavar='INFORMATION ENTITIES', help='', description=''
    )
    retrieve_subparsers.required = True

    # WADO - studies
    retrieve_studies_parser = retrieve_subparsers.add_parser(
        'studies', help='retrieve data for instances of a given study',
        description=(
            'Retrieve data for all DICOM instances of a given DICOM study.'
        ),
        parents=[abstract_required_study_parser]
    )
    retrieve_studies_subparsers = retrieve_studies_parser.add_subparsers(
        dest='studies_resource'
    )
    retrieve_studies_subparsers.required = True

    retrieve_studies_metadata_parser = retrieve_studies_subparsers.add_parser(
        'metadata', description=(
            'Retrieve metadata of DICOM instances of a given DICOM study.'
        ),
        parents=[abstract_fmt_parser, abstract_save_parser]
    )
    retrieve_studies_metadata_parser.set_defaults(
        func=_retrieve_study_metadata
    )

    retrieve_studies_full_parser = retrieve_studies_subparsers.add_parser(
        'full', description=(
            'Retrieve DICOM instances of a given DICOM study.'
        ),
        parents=[abstract_save_parser, abstract_retrieve_parser]
    )
    retrieve_studies_full_parser.set_defaults(func=_retrieve_study)

    # WADO - series
    retrieve_series_parser = retrieve_subparsers.add_parser(
        'series', help='retrieve data for instances of a given series',
        description=(
            'Retrieve data for all DICOM instances of a given DICOM series.'
        ),
        parents=[
            abstract_required_study_parser, abstract_required_series_parser,
        ]
    )
    retrieve_series_subparsers = retrieve_series_parser.add_subparsers(
        dest='series_resource'
    )
    retrieve_series_subparsers.required = True

    retrieve_series_metadata_parser = retrieve_series_subparsers.add_parser(
        'metadata', description=(
            'Retrieve metadata of DICOM instances of a given DICOM series.'
        ),
        parents=[abstract_fmt_parser, abstract_save_parser]
    )
    retrieve_series_metadata_parser.set_defaults(
        func=_retrieve_series_metadata
    )

    retrieve_series_full_parser = retrieve_series_subparsers.add_parser(
        'full', description=(
            'Retrieve DICOM instances of a given DICOM series.'
        ),
        parents=[abstract_save_parser, abstract_retrieve_parser]
    )
    retrieve_series_full_parser.set_defaults(func=_retrieve_series)

    # WADO - instance
    retrieve_instance_parser = retrieve_subparsers.add_parser(
        'instances', help='retrieve data of a given instance',
        description=(
            'Retrieve data for an individual DICOM instance.'
        ),
        parents=[
            abstract_required_study_parser, abstract_required_series_parser,
            abstract_required_instance_parser
        ]
    )
    retrieve_instance_subparsers = retrieve_instance_parser.add_subparsers(
        dest='instances_resource'
    )
    retrieve_instance_subparsers.required = True

    retrieve_instance_metadata_parser = retrieve_instance_subparsers.add_parser(
        'metadata', description=(
            'Retrieve metadata of an invidividual DICOM instance.'
        ),
        parents=[abstract_fmt_parser, abstract_save_parser]
    )
    retrieve_instance_metadata_parser.set_defaults(
        func=_retrieve_instance_metadata
    )

    retrieve_instance_full_parser = retrieve_instance_subparsers.add_parser(
        'full', description=('Retrieve a DICOM instance.'),
        parents=[abstract_save_parser, abstract_retrieve_parser]
    )
    retrieve_instance_full_parser.set_defaults(func=_retrieve_instance)

    retrieve_instance_frames_parser = retrieve_instance_subparsers.add_parser(
        'frames', description=(
            'Retrieve one or more frames of the pixel data element of an '
            'invidividual DICOM instance.'
        ),
        parents=[abstract_save_parser, abstract_retrieve_parser]
    )
    retrieve_instance_frames_parser.add_argument(
        '--numbers', metavar='NUM', type=int, nargs='+', dest='frame_numbers',
        help='frame numbers'
    )
    retrieve_instance_frames_parser.add_argument(
        '--show', action='store_true',
        help='display retrieved images'
    )
    retrieve_instance_frames_parser.set_defaults(func=_retrieve_instance_frames)

    # WADO - bulkdata
    retrieve_bulkdata_parser = retrieve_subparsers.add_parser(
        'bulkdata', help='retrieve bulk data from a known location',
        description=(
            'Retrieve bulk data of a DICOM object from a known location.'
        ),
        parents=[abstract_retrieve_parser]
    )
    retrieve_bulkdata_parser.add_argument(
        '--uri', metavar='URI', dest='bulkdata_uri', required=True,
        help='unique resource identifier of bulk data element'
    )
    retrieve_bulkdata_parser.set_defaults(func=_retrieve_bulkdata)

    # STOW
    store_parser = subparsers.add_parser(
        'store',
        description='STOW-RS: Store Over the Web by RESTful Services.',
    )
    store_subparsers = store_parser.add_subparsers(
        dest='stow_ie', metavar='INFORMATION ENTITIES', help='', description=''
    )
    store_subparsers.required = True

    # STOW - instances
    store_studies_parser = store_subparsers.add_parser(
        'instances', help='store one or more DICOM instances',
        description='Store DICOM instances.',
        parents=[abstract_optional_study_parser, abstract_load_parser]
    )
    store_studies_parser.set_defaults(func=_store_instances)

    return parser


def _parse_search_parameters(args):
    params = dict()
    if args.search_fuzzymatching:
        params['fuzzymatching'] = args.search_fuzzymatching
    params['offset'] = args.search_offset
    params['limit'] = args.search_limit
    params['fields'] = args.search_fields
    params['search_filters'] = {}
    for f in args.search_filters:
        k, v = f.split('=')
        params['search_filters'][k] = v
    return params


def _print_instance(data):
    logger.info('print instance')
    with BytesIO() as fp:
        pydicom.dcmwrite(fp, data)
        output = fp.getvalue()
    print(output)


def _print_metadata(data, prettify=False, dicomize=False):
    logger.info('print metadata')
    if dicomize:
        if isinstance(data, list):
            for ds in data:
                dcm_ds = pydicom.dataset.Dataset.from_json(ds)
                print(dcm_ds)
                print('\n')
        else:
            dcm_ds = pydicom.dataset.Dataset.from_json(data)
            print(dcm_ds)
    elif prettify:
        print(json.dumps(data, indent=4, sort_keys=True))
    else:
        print(json.dumps(data, sort_keys=True))


def _save_instance(data, directory, sop_instance_uid):
    filename = '{}.dcm'.format(sop_instance_uid)
    filepath = os.path.join(directory, filename)
    logger.info('save instance to file: {}'.format(filepath))
    pydicom.dcmwrite(filepath, data)


def _save_metadata(data, directory, sop_instance_uid, prettify=False,
                   dicomize=False):
    if dicomize:
        filename = '{}.dcm'.format(sop_instance_uid)
    else:
        filename = '{}.json'.format(sop_instance_uid)
    filepath = os.path.join(directory, filename)
    logger.info('save metadata to file: {}'.format(filepath))
    if dicomize:
        dataset = pydicom.dataset.Dataset.from_json(data)
        dataset.save_as(filepath)
    else:
        with open(filepath, 'w') as f:
            if prettify:
                json.dump(data, f, indent=4, sort_keys=True)
            else:
                json.dump(data, f, sort_keys=True)


def _print_pixel_data(pixels):
    logger.info('print pixel data')
    print(pixels)
    print('\n')


def _create_headers(args):
    headers = {}
    if hasattr(args, "bearer_token"):
        headers = {
            "Authorization": "Bearer {}".format(args.bearer_token)
        }
    return headers


def _search_for_studies(client, args):
    """Searches for *Studies* and writes metadata to standard output."""
    params = _parse_search_parameters(args)
    studies = client.search_for_studies(**params)
    _print_metadata(studies, args.prettify, args.dicomize)


def _search_for_series(client, args):
    """Searches for Series and writes metadata to standard output."""
    params = _parse_search_parameters(args)
    series = client.search_for_series(args.study_instance_uid, **params)
    _print_metadata(series, args.prettify, args.dicomize)


def _search_for_instances(client, args):
    """Searches for Instances and writes metadata to standard output."""
    params = _parse_search_parameters(args)
    instances = client.search_for_instances(
        args.study_instance_uid, args.series_instance_uid, **params
    )
    _print_metadata(instances, args.prettify, args.dicomize)


def _retrieve_study(client, args):
    """Retrieves all Instances of a given Study and either writes them to
    standard output or to files on disk.
    """
    iterator = client.iter_study(
        args.study_instance_uid,
        media_types=args.media_types,
    )
    for instance in iterator:
        sop_instance_uid = instance.SOPInstanceUID
        if args.save:
            _save_instance(instance, args.output_dir, sop_instance_uid)
        else:
            _print_instance(instance)


def _retrieve_series(client, args):
    """Retrieves all Instances of a given Series and either writes them to
    standard output or to files on disk.
    """
    iterator = client.iter_series(
        args.study_instance_uid, args.series_instance_uid,
        media_types=args.media_types,
    )
    for instance in iterator:
        sop_instance_uid = instance.SOPInstanceUID
        if args.save:
            _save_instance(instance, args.output_dir, sop_instance_uid)
        else:
            _print_instance(instance)


def _retrieve_instance(client, args):
    """Retrieves an Instances and either writes it to standard output or to a
    file on disk.
    """
    instance = client.retrieve_instance(
        args.study_instance_uid, args.series_instance_uid,
        args.sop_instance_uid,
        media_types=args.media_types,
    )
    if args.save:
        _save_instance(instance, args.output_dir, args.sop_instance_uid)
    else:
        _print_instance(instance)


def _retrieve_study_metadata(client, args):
    """Retrieves metadata for all Instances of a given Study and either
    writes it to standard output or to files on disk.
    """
    metadata = client.retrieve_study_metadata(args.study_instance_uid)
    if args.save:
        for md in metadata:
            tag = client.lookup_tag('SOPInstanceUID')
            sop_instance_uid = md[tag]['Value'][0]
            _save_metadata(
                md, args.output_dir, sop_instance_uid, args.prettify,
                args.dicomize
            )
    else:
        _print_metadata(metadata, args.prettify, args.dicomize)


def _retrieve_series_metadata(client, args):
    """Retrieves metadata for all Instances of a given Series and either
    writes it to standard output or to files on disk.
    """
    metadata = client.retrieve_series_metadata(
        args.study_instance_uid, args.series_instance_uid
    )
    if args.save:
        for md in metadata:
            tag = client.lookup_tag('SOPInstanceUID')
            sop_instance_uid = md[tag]['Value'][0]
            _save_metadata(
                md, args.output_dir, sop_instance_uid, args.prettify,
                args.dicomize
            )
    else:
        _print_metadata(metadata, args.prettify, args.dicomize)


def _retrieve_instance_metadata(client, args):
    """Retrieves metadata for an individual Instances and either
    writes it to standard output or to a file on disk.
    """
    metadata = client.retrieve_instance_metadata(
        args.study_instance_uid, args.series_instance_uid,
        args.sop_instance_uid
    )
    if args.save:
        _save_metadata(
            metadata, args.output_dir, args.sop_instance_uid, args.prettify,
            args.dicomize
        )
    else:
        _print_metadata(metadata, args.prettify, args.dicomize)


def _retrieve_instance_frames(client, args):
    """Retrieves frames for an individual instances and either
    writes them to standard output or files on disk or displays them in a GUI
    (depending on the requested content type).
    Frames can only be saved and shown if they are retrieved using
    image media types.
    """
    iterator = client.iter_instance_frames(
        args.study_instance_uid,
        args.series_instance_uid,
        args.sop_instance_uid,
        args.frame_numbers,
        media_types=args.media_types,
    )

    for i, frame in enumerate(iterator):
        if args.save:
            if frame[:2] == b'\xFF\xD8':       # SOI marker => JPEG
                if frame[2:4] == b'\xFF\xF7':  # SOF 55 marker => JPEG-LS
                    extension = 'jls'
                else:
                    extension = 'jpg'
            elif frame[:2] == b'\xFF\x4F':     # SOC marker => JPEG 2000
                extension = 'jp2'
            else:
                extension = 'dat'
            filename = (
                '{sop_instance_uid}_{frame_number}.{extension}'.format(
                    sop_instance_uid=args.sop_instance_uid,
                    frame_number=args.frame_numbers[i],
                    extension=extension
                )
            )
            filepath = os.path.join(args.output_dir, filename)
            with open(filepath, 'bw') as fp:
                fp.write(frame)
        else:
            _print_pixel_data(frame)


def _retrieve_bulkdata(client, args):
    """Retrieves bulk data and either writes them to standard output or to a
    file on disk.
    """
    data = client.retrieve_bulkdata(args.bulkdata_uri, args.media_type)
    print(data)
    print('\n')


def _store_instances(client, args):
    """Loads Instances from files on disk and stores them."""
    datasets = [pydicom.dcmread(f) for f in args.files]
    client.store_instances(datasets)


def _main():
    parser = _get_parser()
    args = parser.parse_args()
    main(args)


def main(args):
    """Main entry point for the ``dicomweb_client`` command line program."""

    configure_logging(args.logging_verbosity)

    if args.username:
        if not args.password:
            message = 'Enter password for user "{0}": '.format(args.username)
            args.password = getpass.getpass(message)
        session = create_session_from_user_pass(args.username, args.password)
    else:
        session = create_session()

    try:
        session = add_certs_to_session(session, args.ca_bundle, args.cert)
        session.headers.update(_create_headers(args))
        client = DICOMwebClient(
            args.url,
            session=session,
            chunk_size=args.chunk_size
        )
        args.func(client, args)
        sys.exit(0)
    except Exception as err:
        logger.error(str(err))
        if args.logging_verbosity > 3:
            tb = traceback.format_exc()
            logger.error(tb)
        sys.exit(1)
