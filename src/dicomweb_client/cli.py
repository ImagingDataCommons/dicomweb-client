'''Command Line Interface (CLI)'''
import os
import sys
import json
import logging
import argparse
import tempfile
import traceback
import getpass
from io import BytesIO

from PIL import Image
import pydicom
import numpy as np

from dicomweb_client.api import DICOMwebClient, load_json_dataset
from dicomweb_client.log import configure_logging


logger = logging.getLogger(__name__)


def _get_parser():
    '''Builds the object for parsing command line arguments.

    Returns
    -------
    argparse.ArgumentParser

    '''
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
        '--url', dest='url', metavar='URL',
        help='uniform resource locator of the DICOMweb service'
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
        parents=[abstract_save_parser]
    )
    retrieve_studies_full_parser.set_defaults(func=_retrieve_study)

    # WADO - series
    retrieve_series_parser = retrieve_subparsers.add_parser(
        'series', help='retrieve data for instances of a given series',
        description=(
            'Retrieve data for all DICOM instances of a given DICOM series.'
        ),
        parents=[
            abstract_required_study_parser, abstract_required_series_parser
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
        parents=[abstract_save_parser]
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
        parents=[abstract_save_parser]
    )
    retrieve_instance_full_parser.set_defaults(func=_retrieve_instance)

    retrieve_instance_frames_parser = retrieve_instance_subparsers.add_parser(
        'frames', description=(
            'Retrieve one or more frames of the pixel data element of an '
            'invidividual DICOM instance.'
        ),
        parents=[abstract_save_parser]
    )
    retrieve_instance_frames_parser.add_argument(
        '--numbers', metavar='NUM', type=int, nargs='+', dest='frame_numbers',
        help='frame numbers'
    )
    retrieve_instance_frames_parser.add_argument(
        '--show', action='store_true',
        help='display retrieved images'
    )
    retrieve_instance_frames_parser.add_argument(
        '--image-format', metavar='NAME', dest='image_format', default=None,
        choices=['jpeg', 'jp2', 'x-jls'],
        help=(
            'name of image format in case frames should be requested '
            'as image media-type (choices: jpeg, jp2, x-jls)'
        )
    )
    retrieve_instance_frames_parser.set_defaults(
        func=_retrieve_instance_frames
    )

    # WADO - bulkdata
    retrieve_bulkdata_parser = retrieve_subparsers.add_parser(
        'bulkdata', help='retrieve bulk data from a known location',
        description=(
            'Retrieve bulk data of a DICOM object from a known location.'
        )
    )
    retrieve_bulkdata_parser.add_argument(
        '--uri', metavar='URI', dest='bulkdata_uri', required=True,
        help='unique resource identifier of bulk data element'
    )
    retrieve_bulkdata_parser.add_argument(
        '--image-format', metavar='NAME', dest='image_format',
        choices=['jpeg', 'x-jls', 'jp2'],
        help=(
            'name of image format in case bulk data should be requested '
            'as image media-type (choices: jpeg, jp2, x-jls)'
        )
    )
    retrieve_bulkdata_parser.set_defaults(
        func=_retrieve_bulkdata
    )

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
                dcm_ds = load_json_dataset(ds)
                print(dcm_ds)
                print('\n')
        else:
            dcm_ds = load_json_dataset(data)
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
        dataset = load_json_dataset(data)
        dataset.save_as(filepath)
    else:
        with open(filepath, 'w') as f:
            if prettify:
                json.dump(data, f, indent=4, sort_keys=True)
            else:
                json.dump(data, f, sort_keys=True)


def _save_frame(image, directory, sop_instance_uid, frame_number):
    filename = (
        '{sop_instance_uid}_frame_{frame_number}.{extension}'.format(
            sop_instance_uid=sop_instance_uid,
            frame_number=frame_number,
            extension=image.format.lower()
        )
    )
    filepath = os.path.join(directory, filename)
    _save_image(image, filepath)


def _save_image(image, filename):
    logger.info('save pixel data to file "{}"'.format(filename))
    image.save(filename)


def _show_image(image):
    logger.info('show pixel data')
    image.show()


def _print_pixeldata(pixels):
    logger.info('print pixel data')
    print(pixels)
    print('\n')


def _search_for_studies(args):
    '''Searches for *Studies* and writes metadata to standard output.'''
    params = _parse_search_parameters(args)
    client = DICOMwebClient(args.url, args.username, args.password)
    studies = client.search_for_studies(**params)
    _print_metadata(studies, args.prettify, args.dicomize)


def _search_for_series(args):
    '''Searches for Series and writes metadata to standard output.'''
    params = _parse_search_parameters(args)
    client = DICOMwebClient(args.url, args.username, args.password)
    series = client.search_for_series(args.study_instance_uid, **params)
    _print_metadata(series, args.prettify, args.dicomize)


def _search_for_instances(args):
    '''Searches for Instances and writes metadata to standard output.'''
    params = _parse_search_parameters(args)
    client = DICOMwebClient(args.url, args.username, args.password)
    instances = client.search_for_instances(
        args.study_instance_uid, args.series_instance_uid, **params
    )
    _print_metadata(instances, args.prettify, args.dicomize)


def _retrieve_study(args):
    '''Retrieves all Instances of a given Study and either writes them to
    standard output or to files on disk.
    '''
    client = DICOMwebClient(args.url, args.username, args.password)
    instances = client.retrieve_study(args.study_instance_uid)
    for inst in instances:
        sop_instance_uid = inst.SOPInstanceUID
        if args.save:
            _save_instance(inst, args.output_dir, sop_instance_uid)
        else:
            _print_instance(inst)


def _retrieve_series(args):
    '''Retrieves all Instances of a given Series and either writes them to
    standard output or to files on disk.
    '''
    client = DICOMwebClient(args.url, args.username, args.password)
    instances = client.retrieve_series(
        args.study_instance_uid, args.series_instance_uid
    )
    for inst in instances:
        sop_instance_uid = inst.SOPInstanceUID
        if args.save:
            _save_instance(inst, args.output_dir, sop_instance_uid)
        else:
            _print_instance(inst)


def _retrieve_instance(args):
    '''Retrieves an Instances and either writes it to standard output or to a
    file on disk.
    '''
    client = DICOMwebClient(args.url, args.username, args.password)
    instance = client.retrieve_instance(
        args.study_instance_uid, args.series_instance_uid,
        args.sop_instance_uid
    )
    if args.save:
        _save_instance(instance, args.output_dir, args.sop_instance_uid)
    else:
        _print_instance(instance)


def _retrieve_study_metadata(args):
    '''Retrieves metadata for all Instances of a given Study and either
    writes it to standard output or to files on disk.
    '''
    client = DICOMwebClient(args.url, args.username, args.password)
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


def _retrieve_series_metadata(args):
    '''Retrieves metadata for all Instances of a given Series and either
    writes it to standard output or to files on disk.
    '''
    client = DICOMwebClient(args.url, args.username, args.password)
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


def _retrieve_instance_metadata(args):
    '''Retrieves metadata for an individual Instances and either
    writes it to standard output or to a file on disk.
    '''
    client = DICOMwebClient(args.url, args.username, args.password)
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


def _retrieve_instance_frames(args):
    '''Retrieves frames for an individual Instances and either
    writes it to standard output or to a file on disk or displays it
    (depending on the requested content type).
    '''
    client = DICOMwebClient(args.url, args.username, args.password)
    pixeldata = client.retrieve_instance_frames(
        args.study_instance_uid, args.series_instance_uid,
        args.sop_instance_uid, args.frame_numbers,
        args.image_format
    )

    for i, data in enumerate(pixeldata):
        if args.save or args.show:
            if args.image_format is None:
                # Pixeldata was returned uncompressed and cannot be converted
                # to an image without additional metadata
                # (Rows, Columns, PixelRepresentation, BitsAllocated).
                raise ValueError(
                    'Cannot load image from uncompressed pixel data without '
                    'additional metadata.'
                )
                # TODO: Retrieve metadata and load pixel data into numpy array
                # https://github.com/pydicom/pydicom/blob/master/pydicom/pixel_data_handlers/numpy_handler.py

                # metadata = client.retrieve_instance_metadata(
                #     args.study_instance_uid, args.series_instance_uid,
                #     args.sop_instance_uid
                # )
                # ds = load_json_dataset(metadata[0])

            elif args.image_format == 'x-jls':
                try:
                    import jpeg_ls
                except ImportError:
                    raise ImportError(
                        'Cannot load image from JPEG-LS compressed pixel data '
                        'because PyCharLS package is not installed.'
                    )
                image = jpeg_ls.decode(np.fromstring(data, dtype=np.uint8))
            else:
                image = Image.open(BytesIO(data))
            if args.save:
                _save_frame(
                    image, args.output_dir, args.sop_instance_uid,
                    args.frame_numbers[i]
                )
            elif args.show:
                _show_image(image)
        else:
            _print_pixeldata(data)


def _retrieve_bulkdata(args):
    '''Retrieves bulk data and either writes them to standard output or to a
    file on disk.
    '''
    client = DICOMwebClient(args.url, args.username, args.password)
    data = client.retrieve_bulkdata(args.bulkdata_uri, args.image_format)
    print(data)
    print('\n')


def _store_instances(args):
    '''Loads Instances from files on disk and stores them.'''
    client = DICOMwebClient(args.url, args.username, args.password)
    datasets = list()
    for f in args.files:
        ds = pydicom.dcmread(f)
        datasets.append(ds)
    client.store_instances(datasets)


def main():
    '''Main entry point for the ``dicomweb_client`` command line program.'''
    parser = _get_parser()
    args = parser.parse_args()

    if args.username:
        if not args.password:
            message = 'Enter password for user "{0}": '.format(args.username)
            args.password = getpass.getpass(message)

    configure_logging(args.logging_verbosity)
    try:
        args.func(args)
        sys.exit(0)
    except Exception as err:
        logger.error(str(err))
        if args.logging_verbosity > 3:
            tb = traceback.format_exc()
            logger.error(tb)
        sys.exit(1)


if __name__ == '__main__':

    main()
