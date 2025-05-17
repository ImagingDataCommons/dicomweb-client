"""Client to access DICOM Part10 files through a layer of abstraction."""
import collections
import io
import logging
import math
import os
import re
import sqlite3
import time
from enum import Enum
from pathlib import Path
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
)
from urllib.parse import urlparse

import numpy as np
import requests
from PIL import Image
from PIL.ImageCms import ImageCmsProfile, createProfile
from pydicom import config as pydicom_config
from pydicom.datadict import dictionary_VR, keyword_for_tag, tag_for_keyword
from pydicom.dataelem import DataElement
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.encaps import encapsulate, get_frame_offsets
from pydicom.errors import InvalidDicomError
from pydicom.filebase import DicomFileLike
from pydicom.filereader import data_element_offset_to_value, dcmread
from pydicom.filewriter import dcmwrite
from pydicom.pixel_data_handlers.numpy_handler import unpack_bits
from pydicom.tag import (
    BaseTag,
    ItemTag,
    SequenceDelimiterTag,
    Tag,
    TupleTag,
)
from pydicom.uid import UID, RLELossless
from pydicom.valuerep import DA, DT, TM

from dicomweb_client.uri import build_query_string, parse_query_parameters

logger = logging.getLogger(__name__)


_FLOAT_PIXEL_DATA_TAGS = {0x7FE00008, 0x7FE00009, }
_UINT_PIXEL_DATA_TAGS = {0x7FE00010, }
_PIXEL_DATA_TAGS = _FLOAT_PIXEL_DATA_TAGS.union(_UINT_PIXEL_DATA_TAGS)

_JPEG_SOI_MARKER = b'\xFF\xD8'  # also JPEG-LS
_JPEG_EOI_MARKER = b'\xFF\xD9'  # also JPEG-LS
_JPEG2000_SOC_MARKER = b'\xFF\x4F'
_JPEG2000_EOC_MARKER = b'\xFF\xD9'
_START_MARKERS = {_JPEG_SOI_MARKER, _JPEG2000_SOC_MARKER}
_END_MARKERS = {_JPEG_EOI_MARKER, _JPEG2000_EOC_MARKER}


def _are_frames_encapsulated(transfer_syntax_uid: str) -> bool:
    """Determine whether frames are compressed.

    Parameters
    ----------
    transfer_syntax_uid: str
        Unique identifier of the transfer syntax

    Returns
    -------
    bool
        Whether frames are compressed

    """
    if transfer_syntax_uid in {
        '1.2.840.10008.1.2',
        '1.2.840.10008.1.2.1',
        '1.2.840.10008.1.2.2',
        '1.2.840.10008.1.2.1.99',
    }:
        return False
    return True


def _enforce_standard_conformance(fn: Callable) -> Callable:
    """Enforce standard conformance during a function call.

    Parameters
    ----------
    fn: Callable
        Function that should be wrapped

    Returns
    -------
    Callable
        Wrapped function

    """
    def wrapper(*args, **kwargs):
        default_reading_mode = int(
            pydicom_config.settings.reading_validation_mode
        )
        default_writing_mode = int(
            pydicom_config.settings.writing_validation_mode
        )
        pydicom_config.settings.reading_validation_mode = pydicom_config.RAISE
        pydicom_config.settings.writing_validation_mode = pydicom_config.RAISE
        result = fn(*args, **kwargs)
        pydicom_config.settings.reading_validation_mode = default_reading_mode
        pydicom_config.settings.writing_validation_mode = default_writing_mode
        return result

    return wrapper


def _read_frame(
    fp: DicomFileLike,
    first_frame_offset: int,
    basic_offset_table: np.ndarray,
    frame_index: int,
    transfer_syntax_uid: str
) -> bytes:
    frame_offset = basic_offset_table[frame_index]
    fp.seek(first_frame_offset + frame_offset, 0)
    try:
        stop_at = basic_offset_table[frame_index + 1] - frame_offset
    except IndexError:
        # For the last frame, there is no next offset available.
        stop_at = -1

    if _are_frames_encapsulated(transfer_syntax_uid):
        n = 0
        # A frame may consist of multiple items (fragments).
        fragments = []
        while True:
            tag = TupleTag(fp.read_tag())
            if n == stop_at or int(tag) == SequenceDelimiterTag:
                break
            if int(tag) != ItemTag:
                raise ValueError(
                    f'Failed to read frame #{frame_index + 1}. '
                    f'Encountered unexpected tag {tag} in Pixel Data element.'
                )
            length = fp.read_UL()
            fragments.append(fp.read(length))
            n += 4 + 4 + length
        return b''.join(fragments)
    else:
        return fp.read(stop_at)


def _decode_frame(
    frame: bytes,
    frame_index: int,
    transfer_syntax_uid: str,
    rows: int,
    columns: int,
    samples_per_pixel: int,
    bits_allocated: int,
    bits_stored: int,
    photometric_interpretation: str,
    pixel_representation: int,
    planar_configuration: Optional[int] = None
) -> np.ndarray:
    """Decode the pixel data of an individual frame item.

    Parameters
    ----------
    index: int
        Zero-based frame index
    value: bytes
        Value of a Frame item

    Returns
    -------
    numpy.ndarray
        Array of decoded pixels of the frame with shape (Rows x Columns)
        in case of a monochrome image or (Rows x Columns x SamplesPerPixel)
        in case of a color image.

    """
    if bits_allocated == 1:
        # Unfortunately, the type of the return value of the unpack_bits()
        # can be changed dynamically via the as_array argument. This causes
        # issues for mypy and requires this workaround.
        unpacked_frame: np.ndarray = unpack_bits(  # type: ignore
            frame,
            as_array=True
        )
        num_pixels_per_frame = rows * columns * samples_per_pixel
        # Determine the nearest whole number of bytes needed to contain
        # 1-bit pixel data. e.g. 10 x 10 1-bit pixels is 100 bits,
        # which are packed into 12.5 -> 13 bytes
        start = int(((frame_index * num_pixels_per_frame / 8) % 1) * 8)
        end = start + num_pixels_per_frame
        pixel_array = unpacked_frame[start:end]
        return pixel_array.reshape(rows, columns)
    else:
        # This hack creates a small dataset containing a Pixel Data element
        # with only a single frame item, which can then be decoded using the
        # existing pydicom API.
        ds = Dataset()
        ds.file_meta = FileMetaDataset()
        ds.file_meta.TransferSyntaxUID = UID(transfer_syntax_uid)
        ds.Rows = rows
        ds.Columns = columns
        ds.SamplesPerPixel = samples_per_pixel
        ds.PhotometricInterpretation = photometric_interpretation
        ds.PixelRepresentation = pixel_representation
        if planar_configuration is not None:
            ds.PlanarConfiguration = planar_configuration
        ds.BitsAllocated = bits_allocated
        ds.BitsStored = bits_stored
        ds.HighBit = bits_stored - 1
        if _are_frames_encapsulated(transfer_syntax_uid):
            ds.PixelData = encapsulate(frames=[frame])
        else:
            ds.PixelData = frame
        return ds.pixel_array


def _get_frame_offsets(
    fp: DicomFileLike,
    number_of_frames: int,
    number_of_pixels_per_frame: int,
    transfer_syntax_uid: str,
    bits_allocated: int
) -> Tuple[int, np.ndarray]:
    pixel_data_offset = fp.tell()
    if _are_frames_encapsulated(transfer_syntax_uid):
        try:
            basic_offset_table = _get_bot(
                fp,
                number_of_frames=number_of_frames,
                transfer_syntax_uid=transfer_syntax_uid
            )
        except Exception as error:
            raise IOError(f'Failed to build Basic Offset Table: "{error}".')
        first_frame_offset = fp.tell()
    else:
        if fp.is_implicit_VR:
            header_offset = 4 + 4  # tag and length
        else:
            header_offset = 4 + 2 + 2 + 4  # tag, VR, reserved, and length
        first_frame_offset = pixel_data_offset + header_offset
        n_pixels = number_of_pixels_per_frame
        bytes_per_frame_uncompressed = n_pixels * bits_allocated / 8
        basic_offset_table = np.array(
            [
                int(math.floor(i * bytes_per_frame_uncompressed))
                for i in range(number_of_frames)
            ],
            dtype=np.uint32
        )
    return (first_frame_offset, basic_offset_table)


def _get_bot(
    fp: DicomFileLike,
    number_of_frames: int,
    transfer_syntax_uid: str
) -> np.ndarray:
    """Read or build the Basic Offset Table (BOT).

    Parameters
    ----------
    fp: pydicom.filebase.DicomFileLike
        Pointer for DICOM PS3.10 file stream positioned at the first byte of
        the Pixel Data element
    number_of_frames: int
        Number of frames contained in the Pixel Data element
    transfer_syntax_uid: str
        Unique identifier of the transfer syntax

    Returns
    -------
    numpy.ndarray
        Offset of each Frame item in bytes from the first byte of the Pixel
        Data element following the BOT item

    Note
    ----
    Moves the pointer to the first byte of the open file following the BOT item
    (the first byte of the first Frame item).

    """
    logger.debug('read Basic Offset Table')
    basic_offset_table = _read_bot(fp)

    first_frame_offset = fp.tell()
    tag = TupleTag(fp.read_tag())
    if int(tag) != ItemTag:
        raise ValueError('Reading of Basic Offset Table failed')
    fp.seek(first_frame_offset, 0)

    # Basic Offset Table item must be present, but it may be empty
    if len(basic_offset_table) == 0:
        logger.debug('Basic Offset Table item is empty')
    if len(basic_offset_table) != number_of_frames:
        logger.debug('build Basic Offset Table item')
        basic_offset_table = _build_bot(
            fp,
            number_of_frames=number_of_frames,
            transfer_syntax_uid=transfer_syntax_uid
        )

    return basic_offset_table


def _read_bot(fp: DicomFileLike) -> np.ndarray:
    """Read the Basic Offset Table (BOT) of an encapsulated Pixel Data element.

    Parameters
    ----------
    fp: pydicom.filebase.DicomFileLike
        Pointer for DICOM PS3.10 file stream positioned at the first byte of
        the Pixel Data element

    Returns
    -------
    numpy.ndarray
        Offset of each Frame item in bytes from the first byte of the Pixel
        Data element following the BOT item

    Note
    ----
    Moves the pointer to the first byte of the open file following the BOT item
    (the first byte of the first Frame item).

    Raises
    ------
    IOError
        When file pointer is not positioned at first byte of Pixel Data element

    """
    tag = TupleTag(fp.read_tag())
    if int(tag) not in _PIXEL_DATA_TAGS:
        raise IOError(
            'Expected file pointer at first byte of Pixel Data element.'
        )
    # Skip Pixel Data element header (tag, VR, length)
    pixel_data_element_value_offset = data_element_offset_to_value(
        fp.is_implicit_VR, 'OB'
    )
    fp.seek(pixel_data_element_value_offset - 4, 1)
    is_empty, offsets = get_frame_offsets(fp)
    return np.array(offsets, dtype=np.uint32)


def _build_bot(
    fp: DicomFileLike,
    number_of_frames: int,
    transfer_syntax_uid: str
) -> np.ndarray:
    """Build a Basic Offset Table (BOT) for an encapsulated Pixel Data element.

    Parameters
    ----------
    fp: pydicom.filebase.DicomFileLike
        Pointer for DICOM PS3.10 file stream positioned at the first byte of
        the Pixel Data element following the empty Basic Offset Table (BOT)
    number_of_frames: int
        Total number of frames in the dataset
    transfer_syntax_uid: str
        Unique identifier of the transfer syntax

    Returns
    -------
    numpy.ndarray
        Offset of each Frame item in bytes from the first byte of the Pixel
        Data element following the BOT item

    Note
    ----
    Moves the pointer back to the first byte of the Pixel Data element
    following the BOT item (the first byte of the first Frame item).

    Raises
    ------
    IOError
        When file pointer is not positioned at first byte of first Frame item
        after Basic Offset Table item or when parsing of Frame item headers
        fails
    ValueError
        When the number of offsets doesn't match the specified number of frames

    """
    initial_position = fp.tell()
    offset_values = []
    current_offset = 0
    i = 0
    while True:
        frame_position = fp.tell()
        tag = TupleTag(fp.read_tag())
        if int(tag) == SequenceDelimiterTag:
            break
        if int(tag) != ItemTag:
            fp.seek(initial_position, 0)
            raise IOError(
                'Building Basic Offset Table (BOT) failed. Expected tag of '
                f'Frame item #{i} at position {frame_position}.'
            )
        length = fp.read_UL()
        if length % 2:
            fp.seek(initial_position, 0)
            raise IOError(
                'Building Basic Offset Table (BOT) failed. '
                f'Length of Frame item #{i} is not a multiple of 2.'
            )
        elif length == 0:
            fp.seek(initial_position, 0)
            raise IOError(
                'Building Basic Offset Table (BOT) failed. '
                f'Length of Frame item #{i} is zero.'
            )

        first_two_bytes = fp.read(2)
        if not fp.is_little_endian:
            first_two_bytes = first_two_bytes[::-1]

        current_offset = frame_position - initial_position
        if transfer_syntax_uid == RLELossless:
            offset_values.append(current_offset)
        else:
            # In case of fragmentation, we only want to get the offsets to the
            # first fragment of a given frame. We can identify those based on
            # the JPEG and JPEG 2000 markers that should be found at the
            # beginning and end of the compressed byte stream.
            if first_two_bytes in _START_MARKERS:
                offset_values.append(current_offset)

        i += 1
        fp.seek(length - 2, 1)  # minus the first two bytes

    if len(offset_values) != number_of_frames:
        raise ValueError(
            'Number of found frame items does not match specified number '
            f'of frames: {len(offset_values)} instead of {number_of_frames}.'
        )
    else:
        basic_offset_table = offset_values

    fp.seek(initial_position, 0)
    return np.array(basic_offset_table, dtype=np.uint32)


class _QueryResourceType(Enum):

    """DICOMweb Query resource types."""

    STUDIES = 'studies'
    SERIES = 'series'
    INSTANCES = 'instances'


def _build_acceptable_media_type_lut(
    media_types: Tuple[Union[str, Tuple[str, str]], ...],
    supported_media_type_lut: Mapping[str, Iterable[str]]
) -> Mapping[str, Iterable[str]]:
    # If no acceptable transfer syntax has been specified, then we just return
    # the instance in whatever transfer syntax is has been stored.  This
    # behavior should be compliant with the standard (Part 18 Section 8.7.3.4):
    # If the Transfer Syntax is not specified in a message, then the Default
    # Transfer Syntax shall be used, unless the origin server has only access
    # to the pixel data in lossy compressed form or the pixel data in a
    # lossless compressed form that is of such length that it cannot be encoded
    # in the Explicit VR Little Endian Transfer Syntax.
    acceptable_media_type_lut = collections.defaultdict(set)
    for m in media_types:
        if isinstance(m, tuple):
            media_type = str(m[0])
            if media_type not in supported_media_type_lut:
                raise ValueError(
                    f'Media type "{media_type}" is not a valid for '
                    'retrieval of instance frames.'
                )
            if len(m) > 1:
                ts_uid = str(m[1])
                if ts_uid not in supported_media_type_lut[media_type]:
                    raise ValueError(
                        f'Transfer syntax "{ts_uid}" is not a valid for '
                        'retrieval of instance frames with media type '
                        f'"{media_type}".'
                    )
                acceptable_media_type_lut[media_type].add(ts_uid)
            else:
                acceptable_media_type_lut[media_type].update(
                    supported_media_type_lut[media_type]
                )
        elif isinstance(m, str):
            media_type = str(m)
            if media_type not in supported_media_type_lut:
                raise ValueError(
                    f'Media type "{media_type}" is not a valid for '
                    'retrieval of instance frames.'
                )
            acceptable_media_type_lut[media_type].update(
                supported_media_type_lut[media_type]
            )
        else:
            raise ValueError('Argument "media_types" is malformatted.')

    return acceptable_media_type_lut


class _DatabaseManager:

    def __init__(
        self,
        url: str,
        db_dir: Path,
        update_db: bool = False,
        recreate_db: bool = False,
        in_memory: bool = False
    ):
        """Instantiate client.

        Parameters
        ----------
        url: pathlib.Path
            Location of the DICOM files. URL with either a ``file://`` scheme.
        db_dir: pathlib.Path
            Path to the directory where database files should be stored
        update_db: bool, optional
            Whether the database should be updated (default: ``False``). If
            ``True``, the client will search `base_dir` recursively for new
            DICOM Part10 files and create database entries for each file.
            The client will further delete any database entries for files that
            no longer exist on the file system.
        recreate_db: bool, optional
            Whether the database should be recreated (default: ``False``). If
            ``True``, the client will search `base_dir` recursively for DICOM
            Part10 files and create database entries for each file.
        in_memory: bool, optional
            Whether the database should only be stored in memory (default:
            ``False``).

        """
        components = urlparse(url)
        self.base_dir = Path(components.path)

        if in_memory:
            filename = ':memory:'
            self._db_file_identifier = filename
            update_db = True
        else:
            filename = '.dicom-file-client.db'
            filepath = db_dir.joinpath(filename)
            if not filepath.exists():
                update_db = True
            self._db_file_identifier = str(filepath)

        self._db_connection_handle: Union[sqlite3.Connection, None] = None
        self._db_cursor_handle: Union[sqlite3.Cursor, None] = None
        if recreate_db:
            self._drop_db()
            update_db = True

        self._create_db()

        # numpy 2 no longer has prod, but Python >= 3.8 does.  We either have
        # one or the other, so use the python math.prod method when available
        # and fall abck to np if not.
        self._prod = getattr(math, 'prod', np.prod)

        self._attributes = {
            _QueryResourceType.STUDIES: self._get_attributes(
                _QueryResourceType.STUDIES
            ),
            _QueryResourceType.SERIES: self._get_attributes(
                _QueryResourceType.SERIES
            ),
            _QueryResourceType.INSTANCES: self._get_attributes(
                _QueryResourceType.INSTANCES
            ),
        }

        if update_db:
            logger.info('updating database...')
            start = time.time()
            self._update_db()
            end = time.time()
            elapsed = round(end - start)
            logger.info(f'updated database in {elapsed} seconds')

    def __getstate__(self) -> dict:
        """Customize state for serialization via pickle module.

        Returns
        -------
        dict
            Contents of the instance that should be serialized

        """
        contents = self.__dict__
        # The database connection and the cached image file readers should
        # (and cannot) be serialized. Therefore, we reset the state of the
        # instance before serialization.
        # This is critical for applications that rely on Python multiprocessing
        # such as PyTorch or TensorFlow.
        try:
            if self._db_cursor_handle is not None:
                self._db_cursor_handle.execute('PRAGMA optimize')
                self._db_cursor_handle.close()
            if self._db_connection_handle is not None:
                self._db_connection_handle.commit()
                self._db_connection_handle.close()
        finally:
            contents['_db_connection_handle'] = None
            contents['_db_cursor_handle'] = None
        return contents

    @property
    def _connection(self) -> sqlite3.Connection:
        """sqlite3.Connection: database connection"""
        def adapt_array(array: np.ndarray) -> sqlite3.Binary:
            buffer = array.astype(np.uint32).tobytes()
            return sqlite3.Binary(buffer)

        def convert_array(buffer: bytes) -> np.ndarray:
            return np.frombuffer(buffer, dtype=np.uint32)

        sqlite3.register_adapter(np.ndarray, adapt_array)
        sqlite3.register_converter('ARRAY', convert_array)

        if self._db_connection_handle is None:
            self._db_connection_handle = sqlite3.connect(
                str(self._db_file_identifier),
                detect_types=sqlite3.PARSE_DECLTYPES
            )
            self._db_connection_handle.row_factory = sqlite3.Row
        return self._db_connection_handle

    @property
    def _cursor(self) -> sqlite3.Cursor:
        if self._db_cursor_handle is None:
            self._db_cursor_handle = self._connection.cursor()
        return self._db_cursor_handle

    def _create_db(self):
        """Creating database tables and indices."""
        with self._connection as connection:
            cursor = connection.cursor()
            cursor.execute('PRAGMA journal_mode = WAL')
            cursor.execute('PRAGMA synchronous = off')
            cursor.execute('PRAGMA temp_store = memory')
            cursor.execute('PRAGMA mmap_size = 30000000000')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS studies (
                    StudyInstanceUID TEXT NOT NULL,
                    StudyID TEXT,
                    StudyDate TEXT,
                    StudyTime TEXT,
                    PatientName TEXT,
                    PatientID TEXT,
                    PatientSex TEXT,
                    PatientBirthDate TEXT,
                    ReferringPhysicianName TEXT,
                    PRIMARY KEY (StudyInstanceUID)
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS study_index_patient_id
                ON studies (PatientID)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS study_index_study_id
                ON studies (StudyID)
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS series (
                    StudyInstanceUID TEXT NOT NULL,
                    SeriesInstanceUID TEXT NOT NULL,
                    Modality VARCHAR(2),
                    AccessionNumber TEXT,
                    SeriesNumber INTEGER,
                    PRIMARY KEY (StudyInstanceUID, SeriesInstanceUID)
                    FOREIGN KEY (StudyInstanceUID)
                    REFERENCES studies(StudyInstanceUID)
                )
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS series_index_modality
                ON series (
                    Modality
                )
            ''')

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS instances (
                    StudyInstanceUID TEXT NOT NULL,
                    SeriesInstanceUID TEXT NOT NULL,
                    SOPInstanceUID TEXT NOT NULL,
                    SOPClassUID TEXT NOT NULL,
                    InstanceNumber INTEGER,
                    Rows INTEGER,
                    Columns INTEGER,
                    NumberOfFrames INTEGER,
                    BitsAllocated INTEGER,
                    BitsStored INTEGER,
                    SamplesPerPixel INTEGER,
                    PhotometricInterpretation TEXT,
                    PixelRepresentation INTEGER,
                    PlanarConfiguration INTEGER,
                    _transfer_syntax_uid TEXT NOT NULL,
                    _file_path TEXT,
                    _first_frame_offset INTEGER,
                    _basic_offset_table ARRAY,
                    PRIMARY KEY (
                        StudyInstanceUID,
                        SeriesInstanceUID,
                        SOPInstanceUID
                    )
                    FOREIGN KEY (SeriesInstanceUID)
                    REFERENCES series(SeriesInstanceUID)
                    FOREIGN KEY (StudyInstanceUID)
                    REFERENCES studies(StudyInstanceUID)
                )
            ''')
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS instances_index_sop_class_uid '
                'ON instances (SOPClassUID)'
            )

            cursor.close()

    def _drop_db(self):
        """Drop database tables and indices."""
        with self._connection as connection:
            cursor = connection.cursor()
            cursor.execute('DROP TABLE IF EXISTS instances')
            cursor.execute('DROP TABLE IF EXISTS series')
            cursor.execute('DROP TABLE IF EXISTS studies')
            cursor.close()

    def _get_file_pointer(
        self,
        buf: Union[io.BufferedReader, io.BytesIO],
        transfer_syntax_uid: str
    ) -> DicomFileLike:
        """Get a pointer to a given image file.

        Parameters
        ----------
        buf: Union[io.BufferedReader, io.BytesIO]
            Buffer of a DICOM file containing a data set of an image
        transfer_syntax_uid: str
            Unique identifier of the transfer syntax

        Returns
        -------
        pydicom.filebase.DicomFileLike
            Pointer to file-like object

        Note
        ----
        Close the file-like object when done reading from it.

        """
        uid = UID(transfer_syntax_uid)
        fp = DicomFileLike(buf)
        fp.is_little_endian = uid.is_little_endian
        fp.is_implicit_VR = uid.is_implicit_VR
        return fp

    def _read_selected_metadata(self, fp: DicomFileLike) -> Dataset:
        """Read selected instance metadata from file.

        Parameters
        ----------
        fp: pydicom.filebase.DicomFileLike
            Pointer to file-like object

        Returns
        -------
        pydicom.dataset.Dataset
            Metadata

        """
        tags: List[int] = [
            tag_for_keyword(attr)  # type: ignore
            for attr in (
                self._attributes[_QueryResourceType.STUDIES] +
                self._attributes[_QueryResourceType.SERIES] +
                self._attributes[_QueryResourceType.INSTANCES]
            )
        ]
        return dcmread(
            fp,
            stop_before_pixels=True,
            specific_tags=tags,
            force=True
        )

    @_enforce_standard_conformance
    def _update_db(self):
        """Update database."""
        indexed_file_paths = set(self._get_indexed_file_paths())
        found_file_paths = set()

        studies = {}
        series = {}
        instances = {}
        n = 100
        for i, file_path in enumerate(self.base_dir.glob('**/*')):
            if not file_path.is_file() or file_path.name == 'DICOMDIR':
                continue

            rel_file_path = file_path.relative_to(self.base_dir)
            found_file_paths.add(file_path)
            if rel_file_path in indexed_file_paths:
                logger.debug(f'skip indexed file {file_path}')
                continue

            logger.debug(f'index file {file_path}')
            # TODO: considering changing default "buffering" for improved
            # performance with network-attached storage systems.
            with open(file_path, 'rb') as f:
                try:
                    ds = self._read_selected_metadata(f)
                except (
                    InvalidDicomError,
                    AttributeError,
                    ValueError,
                    LookupError,
                ):
                    logger.warning(f'failed to read file "{file_path}"')
                    continue

                if not hasattr(ds, 'SOPClassUID'):
                    # This is probably a DICOMDIR file or some other weird thing
                    continue

                try:
                    study_instance_uid = ds.StudyInstanceUID
                    series_instance_uid = ds.SeriesInstanceUID
                    sop_instance_uid = ds.SOPInstanceUID

                    study_metadata = self._extract_study_metadata(ds)
                    studies[study_instance_uid] = study_metadata

                    series_metadata = self._extract_series_metadata(ds)
                    series[series_instance_uid] = series_metadata

                    instance_metadata = self._extract_instance_metadata(ds)
                    if hasattr(ds, 'Rows') and hasattr(ds, 'Columns'):
                        transfer_syntax_uid = ds.file_meta.TransferSyntaxUID
                        first_frame_offset, bot = _get_frame_offsets(
                            fp=self._get_file_pointer(
                                f,
                                transfer_syntax_uid=transfer_syntax_uid
                            ),
                            number_of_frames=int(
                                getattr(ds, 'NumberOfFrames', '1')
                            ),
                            number_of_pixels_per_frame=int(
                                self._prod([  # type: ignore
                                    ds.Rows,
                                    ds.Columns,
                                    ds.SamplesPerPixel,
                                ])
                            ),
                            transfer_syntax_uid=transfer_syntax_uid,
                            bits_allocated=ds.BitsAllocated
                        )
                    else:
                        first_frame_offset = -1
                        bot = np.zeros((0, ), dtype=np.uint32)
                    instances[sop_instance_uid] = (
                        *instance_metadata,
                        str(ds.file_meta.TransferSyntaxUID),
                        str(rel_file_path),
                        first_frame_offset,
                        bot,
                    )
                except (AttributeError, ValueError) as error:
                    logger.warning(
                        f'failed to parse file "{file_path}": {error}'
                    )
                    continue

            if not i % n:
                # Insert every nth iteration to avoid having to read all
                # files again in case the update operation gets interrupted
                self._insert_into_db(
                    studies.values(),
                    series.values(),
                    instances.values()
                )
                studies = {}
                series = {}
                instances = {}

        self._insert_into_db(
            studies.values(),
            series.values(),
            instances.values()
        )

        missing_file_paths = [
            file_path
            for file_path in indexed_file_paths
            if file_path not in found_file_paths
        ]
        self._cleanup_db(missing_file_paths)

    def _get_data_element_value(
        self,
        dataset: Dataset,
        keyword: str
    ) -> Union[str, int, None]:
        try:
            value = getattr(dataset, keyword)
        except (AttributeError, KeyError):
            if keyword == 'NumberOfFrames':
                value = '1'
            else:
                logger.debug(
                    f'could not extract value of element "{keyword}" '
                    'from dataset'
                )
                value = None
        if value is None or isinstance(value, int):
            return value
        # TODO: consider converting date and time to ISO format
        return str(value)

    def _extract_study_metadata(
        self,
        dataset: Dataset
    ) -> Tuple[
        str,
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
        Optional[str],
    ]:
        metadata = [
            self._get_data_element_value(dataset, attr)
            for attr in self._attributes[_QueryResourceType.STUDIES]
        ]
        return tuple(metadata)  # type: ignore

    def _extract_series_metadata(
        self,
        dataset: Dataset
    ) -> Tuple[
        str,
        str,
        str,
        Optional[str],
        Optional[int],
    ]:
        metadata = [
            self._get_data_element_value(dataset, attr)
            for attr in self._attributes[_QueryResourceType.SERIES]
        ]
        return tuple(metadata)  # type: ignore

    def _extract_instance_metadata(
        self,
        dataset: Dataset,
    ) -> Tuple[
            str,
            str,
            str,
            str,
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[int],
            Optional[str],
            Optional[int],
            Optional[int],
    ]:
        metadata = [
            self._get_data_element_value(dataset, attr)
            for attr in self._attributes[_QueryResourceType.INSTANCES]
        ]
        return tuple(metadata)  # type: ignore

    def _insert_into_db(
        self,
        studies: Iterable[
            Tuple[
                str,
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
            ]
        ],
        series: Iterable[
            Tuple[
                str,
                str,
                str,
                Optional[str],
                Optional[int],
            ]
        ],
        instances: Iterable[
            Tuple[
                str,
                str,
                str,
                str,
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[str],
                Optional[int],
                Optional[int],
                str,
                str,
                int,
                np.ndarray
            ]
        ]
    ):
        with self._connection as connection:
            cursor = connection.cursor()
            cursor.executemany(
                'INSERT OR REPLACE INTO studies '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                studies
            )
            cursor.executemany(
                'INSERT OR REPLACE INTO series '
                'VALUES (?, ?, ?, ?, ?)',
                series
            )
            cursor.executemany(
                'INSERT OR REPLACE INTO instances '
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                instances
            )
            cursor.close()

    def _cleanup_db(self, missing_file_paths: Sequence[Path]):
        # Find instances for which database entries should be cleaned up.
        # Perform the query in smaller batches to avoid running into issues
        # with parsing parameters in the SQL statement.
        with self._connection as connection:
            cursor = connection.cursor()
            n = 20
            results = []
            for i in range(0, len(missing_file_paths), n):
                batch = missing_file_paths[i:(i + n)]
                cursor.execute(
                    'SELECT StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID '  # noqa
                    'FROM instances '
                    'WHERE _file_path IN ({sequence})'.format(
                        sequence=','.join(['?'] * len(batch))
                    ),
                    [str(p) for p in batch]
                )
                results += cursor.fetchall()
            cursor.close()

        self.delete_instances(
            uids=[
                (
                    r['StudyInstanceUID'],
                    r['SeriesInstanceUID'],
                    r['SOPInstanceUID'],
                )
                for r in results
            ]
        )

    def delete_instances(
        self,
        uids: Sequence[Tuple[str, str, str]]
    ) -> None:
        with self._connection as connection:
            cursor = connection.cursor()
            # Delete instances as well as any parent series or studies that
            # would be empty after the instances are deleted.
            studies_to_check = set()
            series_to_check = set()
            for study_instance_uid, series_instance_uid, sop_instance_uid in uids:  # noqa
                cursor.executemany(
                    'DELETE FROM instances WHERE SOPInstanceUID=?',
                    sop_instance_uid
                )
                studies_to_check.add(study_instance_uid)
                series_to_check.add((study_instance_uid, series_instance_uid))

            for study_instance_uid, series_instance_uid in series_to_check:
                n_in_series = self._count_instances_in_series(
                    series_instance_uid
                )
                if n_in_series == 0:
                    cursor.executemany(
                        'DELETE FROM series WHERE SeriesInstanceUID=?',
                        series_instance_uid
                    )

            for study_instance_uid in studies_to_check:
                n_in_study = self._count_instances_in_study(
                    study_instance_uid
                )
                if n_in_study == 0:
                    cursor.executemany(
                        'DELETE FROM studies WHERE StudyInstanceUID=?',
                        study_instance_uid
                    )

            cursor.close()

    def _get_attributes(self, resource_type: _QueryResourceType) -> List[str]:
        table = resource_type.value
        self._cursor.execute(f'SELECT * FROM {table} LIMIT 1')
        attributes = [
            item[0] for item in self._cursor.description
            if not item[0].startswith('_')
        ]
        return attributes

    def _get_indexed_file_paths(self) -> List[Path]:
        self._cursor.execute('SELECT _file_path FROM instances')
        results = self._cursor.fetchall()
        return [self.base_dir.joinpath(r['_file_path']) for r in results]

    def _build_query(
        self,
        searchable_keywords: Sequence[str],
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None
    ) -> Tuple[str, Dict[str, Union[int, str]]]:
        if fuzzymatching is None:
            fuzzymatching = False

        if fields is not None:
            logger.warning('argument "fields" is ignored')

        query_expressions = []
        query_params = {}
        if search_filters is not None:
            wildcard_search_vrs = (
                'AE', 'CS', 'LO', 'LT', 'PN', 'SH', 'ST', 'UC', 'UR', 'UT',
            )

            first_filter_expression = True
            for i, (key, value) in enumerate(search_filters.items()):
                filter_expressions = []
                filter_params = {}

                if value is None or len(str(value)) == 0:
                    logger.warning(f'skip search filter "{key}" - empty value')
                    continue

                try:
                    keyword = keyword_for_tag(key)
                    vr = dictionary_VR(key)
                except Exception:
                    keyword = key
                    try:
                        tag = tag_for_keyword(keyword)
                        if tag is None:
                            raise
                        vr = dictionary_VR(tag)
                    except Exception:
                        logger.warning(
                            f'skip search filter "{key}" - not a known '
                            'attribute'
                        )
                        continue

                if keyword not in searchable_keywords:
                    logger.warning(
                        f'skip search filter "{key}" - queries based on this '
                        'attribute are not supported'
                    )
                    continue

                if vr in wildcard_search_vrs:
                    if '*' in value:
                        filter_expressions.append(f'{keyword} LIKE :{keyword}')
                        filter_params[keyword] = value.replace('*', '%')
                    elif '?' in value:
                        filter_expressions.append(f'{keyword} LIKE :{keyword}')
                        filter_params[keyword] = value.replace('?', '_')
                    elif vr == 'PN' and fuzzymatching:
                        filter_expressions.append(f'{keyword} LIKE :{keyword}')
                        filter_params[keyword] = f'%{value}%'
                    else:
                        filter_expressions.append(f'{keyword} = :{keyword}')
                        filter_params[keyword] = str(value)
                else:
                    if vr == 'DA':
                        try:
                            DA(value)
                        except ValueError:
                            logger.warning(
                                f'skip search filter "{key}" - not a valid '
                                f'value for value representation DA: {value}'
                            )
                            continue
                        filter_expressions.append(f'{keyword} LIKE :{keyword}')
                        filter_params[keyword] = f'%{value}'
                    elif vr == 'DT':
                        try:
                            DT(value)
                        except ValueError:
                            logger.warning(
                                f'skip search filter "{key}" - not a valid '
                                f'value for value representation DT: {value}'
                            )
                            continue
                        filter_expressions.append(f'{keyword} LIKE :{keyword}')
                        filter_params[keyword] = f'%{value}'
                    elif vr == 'TM':
                        try:
                            TM(value)
                        except ValueError:
                            logger.warning(
                                f'skip search filter "{key}" - not a valid '
                                f'value for value representation TM: {value}'
                            )
                            continue
                        filter_expressions.append(f'{keyword} LIKE :{keyword}')
                        filter_params[keyword] = f'%{value}'
                    else:
                        filter_expressions.append(f'{keyword} = :{keyword}')
                        filter_params[keyword] = str(value)

                if first_filter_expression:
                    query_expressions.append('WHERE')
                    first_filter_expression = False
                else:
                    query_expressions.append('AND')
                query_expressions.extend(filter_expressions)
                query_params.update(filter_params)

        if limit is not None:
            if limit < 0:
                raise ValueError('Limit must be a positive integer.')
            query_expressions.append('LIMIT :limit')
            query_params['limit'] = limit

        if offset is not None:
            if offset < 0:
                raise ValueError('Offset must be a positive integer.')
            if limit is None:
                query_expressions.append('LIMIT :limit')
                query_params['limit'] = -1
            query_expressions.append('OFFSET :offset')
            query_params['offset'] = offset

        query_string = ' '.join(query_expressions)
        return (query_string, query_params)

    def _get_modalities_in_study(self, study_instance_uid: str) -> List[str]:
        self._cursor.execute(
            'SELECT DISTINCT Modality FROM series '
            'WHERE StudyInstanceUID = :study_instance_uid',
            {'study_instance_uid': study_instance_uid}
        )
        results = self._cursor.fetchall()
        return [r['Modality'] for r in results]

    def get_study_identifiers(self) -> List[str]:
        self._cursor.execute('SELECT StudyInstanceUID FROM studies')
        results = self._cursor.fetchall()
        return [r['StudyInstanceUID'] for r in results]

    def get_series_identifiers(
        self,
        study_instance_uid: Optional[str] = None
    ) -> List[Tuple[str, str]]:
        query_expressions = [
            'SELECT StudyInstanceUID, SeriesInstanceUID FROM series'
        ]
        query_params = {}
        if study_instance_uid is not None:
            query_expressions.append(
                'WHERE StudyInstanceUID = :study_instance_uid'
            )
            query_params['study_instance_uid'] = study_instance_uid

        query_string = ' '.join(query_expressions)

        self._cursor.execute(query_string, query_params)
        results = self._cursor.fetchall()

        return [
            (
                r['StudyInstanceUID'],
                r['SeriesInstanceUID'],
            )
            for r in results
        ]

    def get_instance_identifiers(
        self,
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None
    ) -> List[Tuple[str, str, str]]:
        query_expressions = [
            'SELECT StudyInstanceUID, SeriesInstanceUID, SOPInstanceUID',
            'FROM instances'
        ]
        query_params = {}
        if study_instance_uid is not None:
            query_expressions.append(
                'WHERE StudyInstanceUID = :study_instance_uid'
            )
            query_params['study_instance_uid'] = study_instance_uid
        if series_instance_uid is not None:
            if study_instance_uid is None:
                raise ValueError(
                    'Study Instance UID needs to be specified when '
                    'searching for instances by Series Instance UID.'
                )
            query_expressions.append(
                'AND SeriesInstanceUID = :series_instance_uid'
            )
            query_params['series_instance_uid'] = series_instance_uid

        query_string = ' '.join(query_expressions)

        self._cursor.execute(query_string, query_params)
        results = self._cursor.fetchall()

        return [
            (
                r['StudyInstanceUID'],
                r['SeriesInstanceUID'],
                r['SOPInstanceUID'],
            )
            for r in results
        ]

    def get_instance_file_path(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str
    ) -> Path:
        self._cursor.execute(
            'SELECT _file_path FROM instances '
            'WHERE StudyInstanceUID = :study_instance_uid '
            'AND SeriesInstanceUID = :series_instance_uid '
            'AND SOPInstanceUID = :sop_instance_uid ',
            {
                'study_instance_uid': study_instance_uid,
                'series_instance_uid': series_instance_uid,
                'sop_instance_uid': sop_instance_uid,
            }
        )
        result = self._cursor.fetchone()
        if result is None:
            raise IOError(
                f'Could not find instance "{sop_instance_uid}" of '
                f'series "{series_instance_uid}" and '
                f'study "{study_instance_uid}".'
            )
        return self.base_dir.joinpath(result['_file_path'])

    def _count_series_in_study(self, study_instance_uid: str) -> int:
        self._cursor.execute(
            'SELECT COUNT(SeriesInstanceUID) AS count FROM series '
            'WHERE StudyInstanceUID = :study_instance_uid',
            {'study_instance_uid': study_instance_uid}
        )
        result = self._cursor.fetchone()
        return int(result['count'])

    def _count_instances_in_study(self, study_instance_uid: str) -> int:
        self._cursor.execute(
            'SELECT COUNT(SOPInstanceUID) AS count FROM instances '
            'WHERE StudyInstanceUID = :study_instance_uid',
            {'study_instance_uid': study_instance_uid}
        )
        result = self._cursor.fetchone()
        return int(result['count'])

    def _count_instances_in_series(self, series_instance_uid: str) -> int:
        self._cursor.execute(
            'SELECT COUNT(SOPInstanceUID) AS count FROM instances '
            'WHERE SeriesInstanceUID = :series_instance_uid',
            {
                'series_instance_uid': series_instance_uid,
            }
        )
        result = self._cursor.fetchone()
        return int(result['count'])

    def query_studies(
        self,
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None,
        get_remaining: bool = False
    ) -> List[Dict[str, dict]]:
        """Search for studies.

        Parameters
        ----------
        fuzzymatching: Union[bool, None], optional
            Whether fuzzy semantic matching should be performed
        limit: Union[int, None], optional
            Maximum number of results that should be returned
        offset: Union[int, None], optional
            Number of results that should be skipped
        fields: Union[Sequence[str], None], optional
            Names of fields (attributes) that should be included in results
        search_filters: Union[dict, None], optional
            Search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match
        get_remaining: bool, optional
            Whether remaining results should be included

        Returns
        -------
        List[Dict[str, dict]]
            Studies
            (see `Study Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2>`_)

        """  # noqa: E501
        logger.info('search for studies')
        query_filter_string, query_params = self._build_query(
            searchable_keywords=self._attributes[_QueryResourceType.STUDIES],
            fuzzymatching=fuzzymatching,
            limit=limit,
            offset=offset,
            fields=fields,
            search_filters=search_filters
        )

        query_string = ' '.join([
            'SELECT * FROM studies',
            query_filter_string
        ])
        self._cursor.execute(query_string, query_params)
        results = self._cursor.fetchall()

        collection = []
        for row in results:
            dataset = Dataset()
            for key in row.keys():
                if not key.startswith('_'):
                    setattr(dataset, key, row[key])

            n_series_in_study = self._count_series_in_study(
                study_instance_uid=dataset.StudyInstanceUID
            )
            dataset.NumberOfStudyRelatedSeries = n_series_in_study

            n_instances_in_study = self._count_instances_in_study(
                study_instance_uid=dataset.StudyInstanceUID
            )
            dataset.NumberOfStudyRelatedInstances = n_instances_in_study

            modalities_in_study = self._get_modalities_in_study(
                study_instance_uid=dataset.StudyInstanceUID
            )
            dataset.ModalitiesInStudy = modalities_in_study

            collection.append(dataset.to_json_dict())

        return collection

    def query_series(
        self,
        study_instance_uid: Optional[str] = None,
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None,
        get_remaining: bool = False
    ) -> List[Dict[str, dict]]:
        """Search for series.

        Parameters
        ----------
        study_instance_uid: Union[str, None], optional
            Study Instance UID
        fuzzymatching: Union[bool, None], optional
            Whether fuzzy semantic matching should be performed
        limit: Union[int, None], optional
            Maximum number of results that should be returned
        offset: Union[int, None], optional
            Number of results that should be skipped
        fields: Union[Sequence[str], None], optional
            Names of fields (attributes) that should be included in results
        search_filters: Union[dict, None], optional
            Search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match
        get_remaining: bool, optional
            Whether remaining results should be included

        Returns
        -------
        List[Dict[str, dict]]
            Series
            (see `Series Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2a>`_)

        """  # noqa: E501
        if study_instance_uid is None:
            logger.info('search for series')
        else:
            logger.info(f'search for series of study "{study_instance_uid}"')

        if search_filters is None:
            search_params = {}
        else:
            search_params = dict(search_filters)

        all_series = True
        if study_instance_uid is not None:
            search_params['StudyInstanceUID'] = study_instance_uid
            all_series = False

        searchable_keywords = list(self._attributes[_QueryResourceType.STUDIES])
        searchable_keywords.extend(
            self._attributes[_QueryResourceType.SERIES]
        )
        query_filter_string, query_params = self._build_query(
            searchable_keywords=searchable_keywords,
            fuzzymatching=fuzzymatching,
            limit=limit,
            offset=offset,
            fields=fields,
            search_filters=search_params
        )

        query_filter_string = re.sub(
            r'StudyInstanceUID =',
            'series.StudyInstanceUID =',
            query_filter_string
        )

        if all_series:
            query_string = ' '.join([
                'SELECT * FROM series',
                'INNER JOIN studies',
                'ON series.StudyInstanceUID = studies.StudyInstanceUID',
                query_filter_string
            ])
        else:
            includefields = [
                'Modality',
                'SeriesInstanceUID',
                'SeriesNumber',
            ]
            if fields is not None:
                includefields += [
                    f
                    for f in fields
                    if f in {
                        'StudyInstanceUID',
                        'StudyID',
                        'StudyDate',
                        'StudyTime',
                        'PatientName',
                        'PatientID',
                        'PatientSex',
                        'PatientBirthDate',
                    }
                ]
            includefields_string = ', '.join(includefields)
            includefields_string = includefields_string.replace(
                'StudyInstanceUID',
                'studies.StudyInstanceUID'
            )
            query_string = ' '.join([
                f'SELECT {includefields_string} FROM series',
                'INNER JOIN studies',
                'ON series.StudyInstanceUID = studies.StudyInstanceUID',
                query_filter_string
            ])

        self._cursor.execute(query_string, query_params)
        results = self._cursor.fetchall()

        collection = []
        for row in results:
            dataset = Dataset()
            for key in row.keys():
                if not key.startswith('_'):
                    setattr(dataset, key, row[key])

            if all_series:
                n_series_in_study = self._count_series_in_study(
                    study_instance_uid=dataset.StudyInstanceUID
                )
                dataset.NumberOfStudyRelatedSeries = n_series_in_study

                n_instances_in_study = self._count_instances_in_study(
                    study_instance_uid=dataset.StudyInstanceUID
                )
                dataset.NumberOfStudyRelatedInstances = n_instances_in_study

                modalities_in_study = self._get_modalities_in_study(
                    study_instance_uid=dataset.StudyInstanceUID
                )
                dataset.ModalitiesInStudy = modalities_in_study

            n_instances_in_series = self._count_instances_in_series(
                series_instance_uid=dataset.SeriesInstanceUID,
            )
            dataset.NumberOfSeriesRelatedInstances = n_instances_in_series

            collection.append(dataset.to_json_dict())

        return collection

    def get_instance_info(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str
    ) -> Tuple[Dataset, UID, int, np.ndarray]:
        self._cursor.execute(
            '''
            SELECT * FROM instances
            WHERE StudyInstanceUID = ?
            AND SeriesInstanceUID = ?
            AND SOPInstanceUID = ?
            ''',
            (
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
            )
        )
        row = self._cursor.fetchone()

        dataset = Dataset()
        for key in row.keys():
            if not key.startswith('_'):
                value = row[key]
                setattr(dataset, key, value)
        transfer_syntax_uid = UID(row['_transfer_syntax_uid'])
        first_frame_offset = row['_first_frame_offset']
        basic_offset_table = row['_basic_offset_table']

        return (
            dataset,
            transfer_syntax_uid,
            first_frame_offset,
            basic_offset_table
        )

    def query_instances(
        self,
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None,
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None,
        get_remaining: bool = False
    ) -> List[Dict[str, dict]]:
        """Search for instances.

        Parameters
        ----------
        study_instance_uid: Union[str, None], optional
            Study Instance UID
        series_instance_uid: Union[str, None], optional
            Series Instance UID
        fuzzymatching: Union[bool, None], optional
            Whether fuzzy semantic matching should be performed
        limit: Union[int, None], optional
            Maximum number of results that should be returned
        offset: Union[int, None], optional
            Number of results that should be skipped
        fields: Union[Sequence[str], None], optional
            Names of fields (attributes) that should be included in results
        search_filters: Union[dict, None], optional
            Search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match
        get_remaining: bool, optional
            Whether remaining results should be included

        Returns
        -------
        List[Dict[str, dict]]
            Instances
            (see `Instance Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2b>`_)

        """  # noqa: E501
        if search_filters is None:
            search_params = {}
        else:
            search_params = dict(search_filters)

        all_instances = True
        study_instances = True
        if study_instance_uid is None and series_instance_uid is None:
            logger.info('search for instances')
        else:
            if study_instance_uid is None:
                raise TypeError(
                    'Study Instance UID must be specified if '
                    'Series Instance UID is specified.'
                )
            if series_instance_uid is None:
                all_instances = False
                search_params['StudyInstanceUID'] = study_instance_uid
                logger.info(
                    f'search for instances of study "{study_instance_uid}"'
                )
            else:
                all_instances = False
                study_instances = False
                search_params['StudyInstanceUID'] = study_instance_uid
                search_params['SeriesInstanceUID'] = series_instance_uid
                logger.info(
                    f'search for instances of series "{series_instance_uid}" '
                    f'of study "{study_instance_uid}"'
                )

        searchable_keywords = list(self._attributes[_QueryResourceType.STUDIES])
        searchable_keywords.extend(
            self._attributes[_QueryResourceType.SERIES]
        )
        # Not all attributes that get stored in the database are searchable
        searchable_keywords.extend([
            'SOPInstanceUID',
            'SOPClassUID',
            'InstanceNumber',
            'Rows',
            'Columns',
            'BitsAllocated',
            'NumberOfFrames',
        ])
        query_filter_string, query_params = self._build_query(
            searchable_keywords=searchable_keywords,
            fuzzymatching=fuzzymatching,
            limit=limit,
            offset=offset,
            fields=fields,
            search_filters=search_params
        )

        query_filter_string = re.sub(
            r'StudyInstanceUID =',
            'instances.StudyInstanceUID =',
            query_filter_string
        )
        query_filter_string = re.sub(
            r'SeriesInstanceUID =',
            'instances.SeriesInstanceUID =',
            query_filter_string
        )

        if all_instances:
            query_string = ' '.join([
                'SELECT * FROM instances',
                'INNER JOIN series',
                'ON instances.SeriesInstanceUID = series.SeriesInstanceUID',
                'INNER JOIN studies',
                'ON instances.StudyInstanceUID = studies.StudyInstanceUID',
                query_filter_string
            ])
        else:
            includefields = [
                'SOPClassUID',
                'SOPInstanceUID',
                'InstanceNumber',
                'Rows',
                'Columns',
                'BitsAllocated',
                'NumberOfFrames',
            ]
            if study_instances:
                includefields += [
                    'Modality',
                    'SeriesInstanceUID',
                    'SeriesNumber',
                ]
                if fields is not None:
                    includefields += [
                        f
                        for f in fields
                        if f in {
                            'StudyInstanceUID',
                            'StudyID',
                            'StudyDate',
                            'StudyTime',
                            'PatientName',
                            'PatientID',
                            'PatientSex',
                            'PatientBirthDate',
                        }
                    ]
            else:
                if fields is not None:
                    includefields += [
                        f
                        for f in fields
                        if f in {
                            'StudyInstanceUID',
                            'StudyID',
                            'StudyDate',
                            'StudyTime',
                            'PatientName',
                            'PatientID',
                            'PatientSex',
                            'PatientBirthDate',
                            'Modality',
                            'SeriesInstanceUID',
                            'SeriesNumber',
                        }
                    ]
            includefields_string = ', '.join(includefields)
            includefields_string = includefields_string.replace(
                'SeriesInstanceUID',
                'series.SeriesInstanceUID'
            )
            includefields_string = includefields_string.replace(
                'StudyInstanceUID',
                'studies.StudyInstanceUID'
            )
            query_string = ' '.join([
                f'SELECT {includefields_string} FROM instances',
                'INNER JOIN series',
                'ON instances.SeriesInstanceUID = series.SeriesInstanceUID',
                'INNER JOIN studies',
                'ON instances.StudyInstanceUID = studies.StudyInstanceUID',
                query_filter_string
            ])

        self._cursor.execute(query_string, query_params)
        results = self._cursor.fetchall()

        collection = []
        for row in results:
            dataset = Dataset()
            for key in row.keys():
                if not key.startswith('_'):
                    value = row[key]
                    setattr(dataset, key, value)

            if all_instances:
                n_series_in_study = self._count_series_in_study(
                    study_instance_uid=dataset.StudyInstanceUID
                )
                dataset.NumberOfStudyRelatedSeries = n_series_in_study

                n_instances_in_study = self._count_instances_in_study(
                    study_instance_uid=dataset.StudyInstanceUID
                )
                dataset.NumberOfStudyRelatedInstances = n_instances_in_study

                modalities_in_study = self._get_modalities_in_study(
                    study_instance_uid=dataset.StudyInstanceUID
                )
                dataset.ModalitiesInStudy = modalities_in_study

            if all_instances or study_instances:
                n_instances_in_series = self._count_instances_in_series(
                    series_instance_uid=dataset.SeriesInstanceUID,
                )
                dataset.NumberOfSeriesRelatedInstances = n_instances_in_series

            collection.append(dataset.to_json_dict())

        return collection

    def insert_instances(
        self,
        datasets: Sequence[Dataset]
    ) -> Tuple[List[Tuple[Dataset, Path, bytes]], List[Dataset]]:
        """Insert metadata of one or more DICOM instances.

        Parameters
        ----------
        datasets: Sequence[pydicom.dataset.Dataset]
            Datasets that should be stored

        Returns
        -------
        successes: List[Tuple[pydicom.dataset.Dataset, pathlib.Path, bytes]
            Datasets that can be stored successfully, as well as the path to
            the file where it should be stored and the file's content
        failures: List[pydicom.dataset.Dataset]
            Datasets that cannot be stored

        """
        # We first encode all data sets and temporarily store them in memory
        # before inserting the metadata into the database and writing the data
        # sets to files on disk. This will allow us to "roll back" in case of
        # an error. We may want to consider implementing this in a more
        # sophisticated way in case it becomes a performance bottleneck.
        studies: Dict[
            str,
            Tuple[
                str,
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
                Optional[str],
            ]
        ] = {}
        series: Dict[
            str,
            Tuple[
                str,
                str,
                str,
                Optional[str],
                Optional[int],
            ]
        ] = {}
        instances: Dict[
            str,
            Tuple[
                str,
                str,
                str,
                str,
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[int],
                Optional[str],
                Optional[int],
                Optional[int],
                str,
                str,
                int,
                np.ndarray
            ]
        ] = {}
        successes: List[Tuple[Dataset, Path, bytes]] = []
        failures: List[Dataset] = []
        for ds in datasets:
            logger.info(
                f'store instance "{ds.SOPInstanceUID}" '
                f'of series "{ds.SeriesInstanceUID}" '
                f'of study "{ds.StudyInstanceUID}" '
            )

            try:
                study_instance_uid = ds.StudyInstanceUID
                series_instance_uid = ds.SeriesInstanceUID
                sop_instance_uid = ds.SOPInstanceUID
                rel_file_path = '/'.join([
                    'studies',
                    study_instance_uid,
                    'series',
                    series_instance_uid,
                    'instances',
                    sop_instance_uid
                ])

                study_metadata = self._extract_study_metadata(ds)
                studies[study_instance_uid] = study_metadata

                series_metadata = self._extract_series_metadata(ds)
                series[series_instance_uid] = series_metadata

                instance_metadata = self._extract_instance_metadata(ds)
                with io.BytesIO() as b:
                    transfer_syntax_uid = ds.file_meta.TransferSyntaxUID
                    fp = self._get_file_pointer(
                        b,
                        transfer_syntax_uid=transfer_syntax_uid
                    )
                    dcmwrite(b, ds, write_like_original=False)
                    # The data set needs to be read back (at least partially)
                    # to determine the offset of the Pixel Data element. This
                    # is required to either read or build the Basic Offset Table
                    # for image instances to allow for fast retrieval of frames.
                    fp.seek(0)
                    # One needs to specify at least one tag to satisfy mypy.
                    tag = tag_for_keyword('PatientID')
                    dcmread(
                        fp,
                        specific_tags=[tag],  # type: ignore
                        stop_before_pixels=True
                    )
                    pixel_data_offset = fp.tell()

                    pixel_data_element: Union[DataElement, None] = None
                    for pixel_data_tag in _PIXEL_DATA_TAGS:
                        try:
                            pixel_data_element = ds[pixel_data_tag]
                        except KeyError:
                            continue
                    if pixel_data_element is not None:
                        fp.seek(pixel_data_offset, 0)
                        first_frame_offset, bot = _get_frame_offsets(
                            fp,
                            number_of_frames=int(
                                getattr(ds, 'NumberOfFrames', '1')
                            ),
                            number_of_pixels_per_frame=int(
                                self._prod([  # type: ignore
                                    ds.Rows,
                                    ds.Columns,
                                    ds.SamplesPerPixel
                                ])
                            ),
                            transfer_syntax_uid=ds.file_meta.TransferSyntaxUID,
                            bits_allocated=ds.BitsAllocated
                        )
                    else:
                        first_frame_offset = -1
                        bot = np.zeros((0, ), dtype=np.uint32)

                    fp.seek(0)
                    file_content = fp.read()

                instances[sop_instance_uid] = (
                    *instance_metadata,
                    str(ds.file_meta.TransferSyntaxUID),
                    str(rel_file_path),
                    first_frame_offset,
                    bot,
                )
                file_path = self.base_dir.joinpath(rel_file_path)
                successes.append((ds, file_path, file_content))
            except Exception as error:
                logger.error(
                    f'failed to store instance "{ds.SOPInstanceUID}" '
                    f'of series "{ds.SeriesInstanceUID}" '
                    f'of study "{ds.StudyInstanceUID}": {error}'
                )
                failures.append(ds)

        self._insert_into_db(
            studies.values(),
            series.values(),
            instances.values()
        )

        return (successes, failures)


def _create_client_error(
    method: str,
    url: str,
    reason: str,
    headers: Optional[Dict[str, Any]] = None,
    status_code: int = 400
) -> requests.HTTPError:
    """Raise an IOError for a client error.

    Parameters
    ----------
    method: str
        HTTP method
    url: str
        URL of the HTTP request message
    reason: str
        Reason that should be included in the HTTP response message
    headers: Union[Dict[str, Any], None], optional
        Headers of the HTTP request message
    status_code: int, optional
        Status code of the HTTP response message

    Returns
    -------
    requests.HTTPError
        Error with a message that includes `url`, `reason`, and `status_code`

    """
    if status_code < 400 or status_code >= 500:
        raise ValueError(
            'Status code for client error must be in range [400, 500).'
        )
    error_message = ' '.join([
        f'{status_code} Client Error: ',
        reason,
        'for url: ',
        url
    ])
    request = requests.PreparedRequest()
    request.prepare(
        method=method.upper(),
        url=url,
        headers=headers
    )
    response = requests.Response()
    response.status_code = status_code
    response.request = request
    response.url = url
    response.reason = reason
    return requests.HTTPError(error_message, request=request, response=response)


def _create_server_error(
    method: str,
    url: str,
    reason: str,
    headers: Optional[Dict[str, Any]] = None,
    status_code: int = 500
) -> requests.HTTPError:
    """Raise an IOError for a server error.

    Parameters
    ----------
    method: str
        HTTP method
    url: str
        URL of the HTTP request message
    reason: str
        Reason that should be included in the HTTP response message
    headers: Union[Dict[str, Any], None], optional
        Headers of the HTTP request message
    status_code: int, optional
        Status code of the HTTP response message

    Returns
    -------
    requests.HTTPError
        Error with a message that includes `url`, `reason`, and `status_code`

    """
    if status_code < 500 or status_code >= 600:
        raise ValueError(
            'Status code for client error must be in range [500, 600).'
        )
    error_message = ' '.join([
        f'{status_code} Server Error: ',
        reason,
        'for url: ',
        url
    ])
    request = requests.PreparedRequest()
    request.prepare(
        method=method.upper(),
        url=url,
        headers=headers
    )
    response = requests.Response()
    response.status_code = status_code
    response.request = request
    response.url = url
    response.reason = reason
    return requests.HTTPError(error_message, request=request, response=response)


class DICOMfileClient:

    """Client for managing DICOM Part10 files in a DICOMweb-like manner.

    Facilitates serverless access to data stored locally on a file system as
    DICOM Part10 files.

    Note
    ----
    The class internally uses an in-memory database, which is persisted on disk
    to facilitate faster subsequent data access. However, the implementation
    details of the database and the structure of any database files stored on
    the file system may change at any time and should not be relied on.

    Note
    ----
    This is **not** an implementation of the DICOM File Service and does not
    depend on the presence of ``DICOMDIR`` files.

    Attributes
    ----------
    base_url: str
        Unique resource locator of the DICOMweb service
    scheme: str
        Name of the scheme (``file``)
    protocol: str
        Name of the protocol (``None``)
    url_prefix: str
        URL path prefix for DICOMweb services (part of `base_url`)
    qido_url_prefix: Union[str, None]
        URL path prefix for QIDO-RS (not part of `base_url`)
    wado_url_prefix: Union[str, None]
        URL path prefix for WADO-RS (not part of `base_url`)
    stow_url_prefix: Union[str, None]
        URL path prefix for STOW-RS (not part of `base_url`)
    delete_url_prefix: Union[str, None]
        URL path prefix for DELETE (not part of `base_url`)

    """

    def __init__(
        self,
        url: str,
        update_db: bool = False,
        recreate_db: bool = False,
        in_memory: bool = False,
        db_dir: Optional[Union[Path, str]] = None,
        readonly: bool = False
    ):
        """Instantiate client.

        Parameters
        ----------
        url: str
            Unique resource locator of directory where data is stored
            (must have ``file`` scheme)
        update_db: bool, optional
            Whether the database should be updated (default: ``False``). If
            ``True``, the client will search `base_dir` recursively for new
            DICOM Part10 files and create database entries for each file.
            The client will further delete any database entries for files that
            no longer exist on the file system.
        recreate_db: bool, optional
            Whether the database should be recreated (default: ``False``). If
            ``True``, the client will search `base_dir` recursively for DICOM
            Part10 files and create database entries for each file.
        in_memory: bool, optional
            Whether the database should only be stored in memory (default:
            ``False``).
        db_dir: Union[pathlib.Path, str, None], optional
            Path to directory where database files should be stored (defaults
            to `base_dir`)
        readonly: bool, optional
            Whether data should be considered read-only. Attempts to store or
            delete data will be denied.

        """
        self.base_url = url
        components = urlparse(url)
        self.scheme = components.scheme
        if self.scheme != 'file':
            raise ValueError(f'URL scheme "{self.scheme}" is not supported.')
        self.protocol = None
        self.url_prefix = components.path
        self.qido_url_prefix = None
        self.wado_url_prefix = None
        self.stow_url_prefix = None
        self.delete_url_prefix = None
        if db_dir is None:
            base_dir = Path(components.path)
            db_dir = base_dir
        else:
            db_dir = Path(db_dir).resolve()
        self._db_manager = _DatabaseManager(
            url=url,
            db_dir=db_dir,
            update_db=update_db,
            recreate_db=recreate_db,
            in_memory=in_memory
        )
        self._readonly = readonly

    def _get_image_file_pointer(
        self,
        file_path: Path,
        transfer_syntax_uid: str
    ) -> DicomFileLike:
        """Get a pointer to a given image file.

        Parameters
        ----------
        file_path: pathlib.Path
            Path to a DICOM file containing a data set of an image
        transfer_syntax_uid: str
            Unique identifier of the transfer syntax

        Returns
        -------
        pydicom.filebase.DicomFileLike
            Pointer to file-like object

        Note
        ----
        Close the file-like object when done reading from it.

        """
        uid = UID(transfer_syntax_uid)
        fp = DicomFileLike(open(file_path, 'rb'))
        fp.is_little_endian = uid.is_little_endian
        fp.is_implicit_VR = uid.is_implicit_VR
        return fp

    def _get_studies_url(
        self,
        study_instance_uid: Optional[str] = None
    ) -> str:
        """Construct URL for study-level requests.

        Parameters
        ----------
        study_instance_uid: Union[str, None], optional
            Study Instance UID

        Returns
        -------
        str
            URL

        """
        if study_instance_uid is not None:
            return f'{self.base_url}/studies/{study_instance_uid}'
        return f'{self.base_url}/studies'

    def _get_series_url(
        self,
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None
    ) -> str:
        """Construct URL for series-level requests.

        Parameters
        ----------
        study_instance_uid: Union[str, None], optional
            Study Instance UID
        series_instance_uid: Union[str, None], optional
            Series Instance UID

        Returns
        -------
        str
            URL

        """
        if study_instance_uid is not None:
            url = self._get_studies_url(study_instance_uid)
            if series_instance_uid is not None:
                url += f'/series/{series_instance_uid}'
            else:
                url += '/series'
            return url
        else:
            if series_instance_uid is not None:
                logger.warning(
                    'series UID is ignored because study UID is undefined'
                )
            return f'{self.base_url}/series'

    def _get_instances_url(
        self,
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None,
        sop_instance_uid: Optional[str] = None
    ) -> str:
        """Construct URL for instance-level requests.

        Parameters
        ----------
        study_instance_uid: Union[str, None], optional
            Study Instance UID
        series_instance_uid: Union[str, None], optional
            Series Instance UID
        sop_instance_uid: Union[str, None], optional
            SOP Instance UID

        Returns
        -------
        str
            URL

        """
        if study_instance_uid is not None and series_instance_uid is not None:
            url = self._get_series_url(study_instance_uid, series_instance_uid)
            url += '/instances'
            if sop_instance_uid is not None:
                url += f'/{sop_instance_uid}'
            return url
        elif study_instance_uid is not None:
            if sop_instance_uid is not None:
                logger.warning(
                    'SOP Instance UID is ignored because Series Instance UID '
                    'is undefined'
                )
            url = self._get_studies_url(study_instance_uid)
            url += '/instances'
            return url
        else:
            if sop_instance_uid is not None:
                logger.warning(
                    'SOP Instance UID is ignored because Study/Series '
                    'Instance UID are undefined'
                )
            return f'{self.base_url}/instances'

    def search_for_studies(
        self,
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None,
        get_remaining: bool = False
    ) -> List[Dict[str, dict]]:
        """Search for studies.

        Parameters
        ----------
        fuzzymatching: Union[bool, None], optional
            Whether fuzzy semantic matching should be performed
        limit: Union[int, None], optional
            Maximum number of results that should be returned
        offset: Union[int, None], optional
            Number of results that should be skipped
        fields: Union[Sequence[str], None], optional
            Names of fields (attributes) that should be included in results
        search_filters: Union[dict, None], optional
            Search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match
        get_remaining: bool, optional
            Whether remaining results should be included

        Returns
        -------
        List[Dict[str, dict]]
            Studies
            (see `Study Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2>`_)

        Note
        ----
        No additional `fields` are currently supported.

        """  # noqa: E501
        try:
            return self._db_manager.query_studies(
                fuzzymatching=fuzzymatching,
                limit=limit,
                offset=offset,
                fields=fields,
                search_filters=search_filters
            )
        except Exception as error:
            url = self._get_studies_url()
            params = parse_query_parameters(
                fuzzymatching=fuzzymatching,
                limit=limit,
                offset=offset,
                fields=fields,
                search_filters=search_filters
            )
            url += build_query_string(params)
            raise _create_server_error(
                method='GET',
                url=url,
                reason=str(error)
            )

    def search_for_series(
        self,
        study_instance_uid: Optional[str] = None,
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None,
        get_remaining: bool = False
    ) -> List[Dict[str, dict]]:
        """Search for series.

        Parameters
        ----------
        study_instance_uid: Union[str, None], optional
            Study Instance UID
        fuzzymatching: Union[bool, None], optional
            Whether fuzzy semantic matching should be performed
        limit: Union[int, None], optional
            Maximum number of results that should be returned
        offset: Union[int, None], optional
            Number of results that should be skipped
        fields: Union[Sequence[str], None], optional
            Names of fields (attributes) that should be included in results
        search_filters: Union[dict, None], optional
            Search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match
        get_remaining: bool, optional
            Whether remaining results should be included

        Returns
        -------
        List[Dict[str, dict]]
            Series
            (see `Series Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2a>`_)

        """  # noqa: E501
        try:
            return self._db_manager.query_series(
                study_instance_uid=study_instance_uid,
                fuzzymatching=fuzzymatching,
                limit=limit,
                offset=offset,
                fields=fields,
                search_filters=search_filters
            )
        except Exception as error:
            url = self._get_series_url(study_instance_uid)
            params = parse_query_parameters(
                fuzzymatching=fuzzymatching,
                limit=limit,
                offset=offset,
                fields=fields,
                search_filters=search_filters
            )
            url += build_query_string(params)
            raise _create_server_error(
                method='GET',
                url=url,
                reason=str(error)
            )

    def search_for_instances(
        self,
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None,
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None,
        get_remaining: bool = False
    ) -> List[Dict[str, dict]]:
        """Search for instances.

        Parameters
        ----------
        study_instance_uid: Union[str, None], optional
            Study Instance UID
        series_instance_uid: Union[str, None], optional
            Series Instance UID
        fuzzymatching: Union[bool, None], optional
            Whether fuzzy semantic matching should be performed
        limit: Union[int, None], optional
            Maximum number of results that should be returned
        offset: Union[int, None], optional
            Number of results that should be skipped
        fields: Union[Sequence[str], None], optional
            Names of fields (attributes) that should be included in results
        search_filters: Union[dict, None], optional
            Search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match
        get_remaining: bool, optional
            Whether remaining results should be included

        Returns
        -------
        List[Dict[str, dict]]
            Instances
            (see `Instance Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2b>`_)

        Note
        ----
        No additional `fields` are currently supported.

        """  # noqa: E501
        try:
            return self._db_manager.query_instances(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                fuzzymatching=fuzzymatching,
                limit=limit,
                offset=offset,
                fields=fields,
                search_filters=search_filters
            )
        except Exception as error:
            url = self._get_instances_url(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid
            )
            params = parse_query_parameters(
                fuzzymatching=fuzzymatching,
                limit=limit,
                offset=offset,
                fields=fields,
                search_filters=search_filters
            )
            url += build_query_string(params)
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def retrieve_bulkdata(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None,
        byte_range: Optional[Tuple[int, int]] = None
    ) -> List[bytes]:
        """Retrieve bulk data at a given location.

        Parameters
        ----------
        url: str
            Location of the bulk data
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        byte_range: Union[Tuple[int, int], None], optional
            Start and end of byte range

        Returns
        -------
        Iterator[bytes]
            Bulk data items

        Raises
        ------
        IOError
            When requested resource is not found at `url`

        """  # noqa: E501
        try:
            iterator = self.iter_bulkdata(
                url=url,
                media_types=media_types,
                byte_range=byte_range
            )
            return list(iterator)
        except requests.HTTPError:
            raise
        except Exception as error:
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def iter_bulkdata(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None,
        byte_range: Optional[Tuple[int, int]] = None
    ) -> Iterator[bytes]:
        """Iterate over bulk data items at a given location.

        Parameters
        ----------
        url: str
            Location of the bulk data
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        byte_range: Union[Tuple[int, int], None], optional
            Start and end of byte range

        Returns
        -------
        Iterator[bytes]
            Bulk data items

        Raises
        ------
        IOError
            When requested resource is not found at `url`

        """  # noqa: E501
        # The retrieve_study_metadata, retrieve_series_metadata, and
        # retrieve_instance_metadata methods currently include all bulkdata
        # into metadata resources by value rather than by reference, i.e.,
        # using the "InlineBinary" rather than the "BulkdataURI" key.
        # Therefore, no valid URL should exist for any bulkdata at this point.
        # If that behavior gets changed, i.e., if bulkdata gets included into
        # metadata using "BulkdataURI", then the implementation of this method
        # will need to change as well.
        raise _create_client_error(
            method='GET',
            url=url,
            reason='Resource does not exist.',
            status_code=404
        )

    def retrieve_study_metadata(
        self,
        study_instance_uid: str,
    ) -> List[Dict[str, dict]]:
        """Retrieve metadata of instances in a study.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID

        Returns
        -------
        List[Dict[str, Any]]
            Metadata of each instance in study

        """
        try:
            logger.info(
                'retrieve metadata of all instances '
                f'of study "{study_instance_uid}"'
            )
            series_identifiers = self._db_manager.get_series_identifiers(
                study_instance_uid=study_instance_uid
            )
            collection = []
            for study_instance_uid, series_instance_uid in series_identifiers:
                collection.extend(
                    self.retrieve_series_metadata(
                        study_instance_uid=study_instance_uid,
                        series_instance_uid=series_instance_uid,
                    )
                )
            return collection
        except Exception as error:
            url = self._get_studies_url(study_instance_uid)
            url += '/metadata'
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def iter_study(
        self,
        study_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None
    ) -> Iterator[Dataset]:
        """Iterate over all instances of a study.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        Iterator[pydicom.dataset.Dataset]
            Instances

        """  # noqa: E501
        logger.info(
            f'iterate over all instances of study "{study_instance_uid}"'
        )
        try:
            for (
                study_instance_uid,
                series_instance_uid,
            ) in self._db_manager.get_series_identifiers(
                study_instance_uid=study_instance_uid
            ):
                for (
                    study_instance_uid,
                    series_instance_uid,
                    sop_instance_uid,
                ) in self._db_manager.get_instance_identifiers(
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                ):
                    yield self.retrieve_instance(
                        study_instance_uid=study_instance_uid,
                        series_instance_uid=series_instance_uid,
                        sop_instance_uid=sop_instance_uid,
                        media_types=media_types
                    )
        except requests.HTTPError:
            raise
        except Exception as error:
            url = self._get_studies_url(study_instance_uid)
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def retrieve_study(
        self,
        study_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None
    ) -> List[Dataset]:
        """Retrieve all instances of a study.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        Sequence[pydicom.dataset.Dataset]
            Instances

        """  # noqa: E501
        logger.info(f'retrieve all instances of study "{study_instance_uid}"')
        try:
            iterator = self.iter_study(
                study_instance_uid=study_instance_uid,
                media_types=media_types,
            )
            return list(iterator)
        except requests.HTTPError:
            raise
        except Exception as error:
            url = self._get_studies_url(study_instance_uid)
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def iter_series(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None
    ) -> Iterator[Dataset]:
        """Iterate over all instances of a series.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        Iterator[pydicom.dataset.Dataset]
            Instances

        """  # noqa: E501
        logger.info(
            f'iterate over all instances of series "{series_instance_uid}" '
            f'of study "{study_instance_uid}"'
        )
        try:
            for (
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
            ) in self._db_manager.get_instance_identifiers(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
            ):
                yield self.retrieve_instance(
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                    sop_instance_uid=sop_instance_uid,
                    media_types=media_types
                )
        except requests.HTTPError:
            raise
        except Exception as error:
            url = self._get_series_url(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
            )
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def retrieve_series(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None
    ) -> List[Dataset]:
        """Retrieve all instances of a series.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        Sequence[pydicom.dataset.Dataset]
            Instances

        """  # noqa: E501
        logger.info(
            f'retrieve all instances of series "{series_instance_uid}" '
            f'of study "{study_instance_uid}"'
        )
        try:
            iterator = self.iter_series(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                media_types=media_types,
            )
            return list(iterator)
        except requests.HTTPError:
            raise
        except Exception as error:
            url = self._get_series_url(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
            )
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def retrieve_series_rendered(
        self, study_instance_uid,
        series_instance_uid,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> bytes:
        """Retrieve rendered representation of a series.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types (choices: ``"image/jpeg"``, ``"image/jp2"``,
            ``"image/gif"``, ``"image/png"``, ``"video/gif"``, ``"video/mp4"``,
            ``"video/h265"``, ``"text/html"``, ``"text/plain"``,
            ``"text/xml"``, ``"text/rtf"``, ``"application/pdf"``)
        params: Union[Dict[str, Any], None], optional
            Additional parameters relevant for given `media_type`,
            e.g., ``{"quality": 95}`` for ``"image/jpeg"``

        Returns
        -------
        bytes
            Rendered representation of series

        """  # noqa: E501
        url = self._get_series_url(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
        )
        url += '/rendered'
        raise _create_server_error(
            method='GET',
            url=url,
            reason='Retrieval of rendered series is not supported.',
            status_code=501
        )

    def retrieve_series_metadata(
        self,
        study_instance_uid: str,
        series_instance_uid: str
    ) -> List[Dict[str, dict]]:
        """Retrieve metadata of instances in a series.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID

        Returns
        -------
        List[Dict[str, Any]]
            Metadata of each instance in series

        """
        logger.info(
            'retrieve metadata of all instances of '
            f'series "{series_instance_uid}" of study "{study_instance_uid}"'
        )
        try:
            return [
                self.retrieve_instance_metadata(
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                    sop_instance_uid=sop_instance_uid,
                )
                for (
                    study_instance_uid,
                    series_instance_uid,
                    sop_instance_uid,
                ) in self._db_manager.get_instance_identifiers(
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                )
            ]
        except requests.HTTPError:
            raise
        except Exception as error:
            url = self._get_series_url(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
            )
            url += '/metadata'
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def retrieve_instance_metadata(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
    ) -> Dict[str, dict]:
        """Retrieve metadata of a single instance.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        sop_instance_uid: str
            SOP Instance UID

        Returns
        -------
        Dict[str, Any]
            Metadata of instance

        """
        logger.info(
            f'retrieve metadata of instance "{sop_instance_uid}" of '
            f'series "{series_instance_uid}" of study "{study_instance_uid}"'
        )
        try:
            file_path = self._db_manager.get_instance_file_path(
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
            )
            metadata = dcmread(file_path, stop_before_pixels=True)
            return metadata.to_json_dict()
        except Exception as error:
            url = self._get_instances_url(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                sop_instance_uid=sop_instance_uid
            )
            url += '/metadata'
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def retrieve_instance(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None
    ) -> Dataset:
        """Retrieve metadata of a single instance.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        sop_instance_uid: str
            SOP Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        pydicom.dataset.Dataset
            Instance

        """  # noqa: E501
        logger.info(
            f'retrieve instance "{sop_instance_uid}" of '
            f'series "{series_instance_uid}" of study "{study_instance_uid}"'
        )

        transfer_syntax_uid_lut = {
            '1.2.840.10008.1.2.1': 'application/dicom',
            '1.2.840.10008.1.2.4.50': 'application/dicom',
            '1.2.840.10008.1.2.4.51': 'application/dicom',
            '1.2.840.10008.1.2.4.57': 'application/dicom',
            '1.2.840.10008.1.2.4.70': 'application/dicom',
            '1.2.840.10008.1.2.4.80': 'application/dicom',
            '1.2.840.10008.1.2.4.81': 'application/dicom',
            '1.2.840.10008.1.2.4.90': 'application/dicom',
            '1.2.840.10008.1.2.4.91': 'application/dicom',
            '1.2.840.10008.1.2.4.92': 'application/dicom',
            '1.2.840.10008.1.2.4.93': 'application/dicom',
            '1.2.840.10008.1.2.5': 'application/dicom',
        }

        supported_media_type_lut = {
            'application/dicom': {
                '1.2.840.10008.1.2.1',
                '1.2.840.10008.1.2.4.50',
                '1.2.840.10008.1.2.4.51',
                '1.2.840.10008.1.2.4.57',
                '1.2.840.10008.1.2.4.70',
                '1.2.840.10008.1.2.4.80',
                '1.2.840.10008.1.2.4.81',
                '1.2.840.10008.1.2.4.90',
                '1.2.840.10008.1.2.4.91',
                '1.2.840.10008.1.2.4.92',
                '1.2.840.10008.1.2.4.93',
                '1.2.840.10008.1.2.5',
                '*',
            },
        }
        supported_media_type_lut['application/*'] = set(
            supported_media_type_lut['application/dicom']
        )
        supported_media_type_lut['application/'] = set(
            supported_media_type_lut['application/dicom']
        )
        supported_media_type_lut['*/*'] = set(
            supported_media_type_lut['application/dicom']
        )
        supported_media_type_lut['*/'] = set(
            supported_media_type_lut['application/dicom']
        )

        if media_types is None:
            media_types = (('application/dicom', '*'), )

        acceptable_media_type_lut = _build_acceptable_media_type_lut(
            media_types,
            supported_media_type_lut
        )
        url = self._get_instances_url(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid
        )

        try:
            file_path = self._db_manager.get_instance_file_path(
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
            )
            dataset = dcmread(file_path)
            transfer_syntax_uid = dataset.file_meta.TransferSyntaxUID

            # Check whether the expected media is specified as one of the
            # acceptable media types.
            expected_media_type = transfer_syntax_uid_lut[transfer_syntax_uid]
            found_matching_media_type = False
            wildcards = {'*/*', '*/', 'application/*', 'application/'}
            if any([w in acceptable_media_type_lut for w in wildcards]):
                found_matching_media_type = True
            elif expected_media_type in acceptable_media_type_lut:
                found_matching_media_type = True
                # If expected media type is specified as one of the acceptable
                # media types, check whether the corresponding transfer syntax
                # is appropriate.
                expected_transfer_syntaxes = acceptable_media_type_lut[
                    expected_media_type
                ]
                if (
                    transfer_syntax_uid not in expected_transfer_syntaxes and
                    '*' not in expected_transfer_syntaxes
                ):
                    raise _create_client_error(
                        method='GET',
                        url=url,
                        reason=(
                            'Instance cannot be retrieved using media type '
                            f'"{expected_media_type}" '
                            'with any of the specified transfer syntaxes: '
                            '"{}".'.format(
                                '", "'.join(expected_transfer_syntaxes)
                            )
                        ),
                        status_code=406
                    )

            if not found_matching_media_type:
                raise _create_client_error(
                    method='GET',
                    url=url,
                    reason=(
                        'Instance cannot be retrieved using any of the '
                        f'acceptable media types: {media_types}.'
                    ),
                    status_code=406
                )
            return dataset

        except requests.HTTPError:
            raise
        except Exception as error:
            raise _create_server_error(
                method='GET',
                url=url,
                reason=str(error)
            )

    def retrieve_instance_rendered(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> bytes:
        """Retrieve an individual, server-side rendered instance.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        sop_instance_uid: str
            SOP Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types (choices: ``"image/jpeg"``, ``"image/jp2"``,
            ``"image/gif"``, ``"image/png"``, ``"video/gif"``, ``"video/mp4"``,
            ``"video/h265"``, ``"text/html"``, ``"text/plain"``,
            ``"text/xml"``, ``"text/rtf"``, ``"application/pdf"``)
        params: Union[Dict[str, Any], None], optional
            Additional parameters relevant for given `media_type`,
            e.g., ``{"quality": 95}`` for ``"image/jpeg"``

        Returns
        -------
        bytes
            Rendered representation of instance

        Note
        ----
        Only rendering of single-frame image instances is currently supported.

        """  # noqa: E501
        url = self._get_instances_url(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid
        )
        url += '/metadata'
        try:
            file_path = self._db_manager.get_instance_file_path(
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
            )
            metadata, transfer_syntax_uid, \
                first_frame_offset, bot = self._db_manager.get_instance_info(
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                    sop_instance_uid=sop_instance_uid
                )
            image_file_pointer = self._get_image_file_pointer(
                file_path,
                transfer_syntax_uid=transfer_syntax_uid
            )
            if int(getattr(metadata, 'NumberOfFrames', '1')) > 1:
                raise ValueError(
                    'Rendering of multi-frame image instance is not supported.'
                )
            frame_index = 0
            frame = _read_frame(
                image_file_pointer,
                first_frame_offset=first_frame_offset,
                basic_offset_table=bot,
                frame_index=frame_index,
                transfer_syntax_uid=transfer_syntax_uid
            )
            image_file_pointer.close()

            # TODO: ICC Profile
            codec_name, codec_kwargs = self._get_image_codec_parameters(
                transfer_syntax_uid=transfer_syntax_uid,
                media_types=media_types,
                params=params
            )

            if codec_name is None:
                return frame

            array = _decode_frame(
                frame=frame,
                frame_index=frame_index,
                transfer_syntax_uid=transfer_syntax_uid,
                rows=metadata.Rows,
                columns=metadata.Columns,
                samples_per_pixel=metadata.SamplesPerPixel,
                bits_allocated=metadata.BitsAllocated,
                bits_stored=metadata.BitsStored,
                photometric_interpretation=metadata.PhotometricInterpretation,
                pixel_representation=metadata.PixelRepresentation,
                planar_configuration=getattr(
                    metadata,
                    'PlanarConfiguration',
                    None
                )
            )
            image = Image.fromarray(array)
            with io.BytesIO() as fp:
                image.save(fp, codec_name, **codec_kwargs)  # type: ignore
                fp.seek(0)
                reencoded_frame = fp.read()
            return reencoded_frame

        except Exception as error:
            raise _create_server_error(
                method='GET',
                url=url,
                reason=str(error)
            )

    def _check_media_types_for_instance_frames(
        self,
        url: str,
        transfer_syntax_uid: UID,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None
    ) -> Union[str, None]:
        transfer_syntax_uid_lut = {
            '1.2.840.10008.1.2.1': 'application/octet-stream',
            '1.2.840.10008.1.2.4.50': 'image/jpeg',
            '1.2.840.10008.1.2.4.51': 'image/jpeg',
            '1.2.840.10008.1.2.4.57': 'image/jpeg',
            '1.2.840.10008.1.2.4.70': 'image/jpeg',
            '1.2.840.10008.1.2.4.80': 'image/jls',
            '1.2.840.10008.1.2.4.81': 'image/jls',
            '1.2.840.10008.1.2.4.90': 'image/jp2',
            '1.2.840.10008.1.2.4.91': 'image/jp2',
            '1.2.840.10008.1.2.4.92': 'image/jpx',
            '1.2.840.10008.1.2.4.93': 'image/jpx',
        }

        supported_media_type_lut = {
            'image/jpeg': {
                '1.2.840.10008.1.2.4.50',
                '1.2.840.10008.1.2.4.51',
                '1.2.840.10008.1.2.4.57',
                '1.2.840.10008.1.2.4.70',
                '*',
            },
            'image/jls': {
                '1.2.840.10008.1.2.4.80',
                '1.2.840.10008.1.2.4.81',
                '*',
            },
            'image/jp2': {
                '1.2.840.10008.1.2.4.90',
                '1.2.840.10008.1.2.4.91',
                '*',
            },
            'image/jpx': {
                '1.2.840.10008.1.2.4.92',
                '1.2.840.10008.1.2.4.93',
                '*',
            },
            'application/octet-stream': {
                '1.2.840.10008.1.2.1',
                '*',
            },
        }
        supported_media_type_lut['image/*'] = set().union(*[
            supported_media_type_lut['image/jpeg'],
            supported_media_type_lut['image/jls'],
            supported_media_type_lut['image/jp2'],
            supported_media_type_lut['image/jpx'],
        ])
        supported_media_type_lut['image/'] = set().union(*[
            supported_media_type_lut['image/jpeg'],
            supported_media_type_lut['image/jls'],
            supported_media_type_lut['image/jp2'],
            supported_media_type_lut['image/jpx'],
        ])
        supported_media_type_lut['application/*'] = set().union(*[
            supported_media_type_lut['application/octet-stream'],
        ])
        supported_media_type_lut['application/'] = set().union(*[
            supported_media_type_lut['application/octet-stream'],
        ])
        supported_media_type_lut['*/*'] = set().union(*[
            supported_media_type_lut['image/*'],
            supported_media_type_lut['application/*'],
        ])
        supported_media_type_lut['*/'] = set().union(*[
            supported_media_type_lut['image/*'],
            supported_media_type_lut['application/*'],
        ])

        if media_types is None:
            media_types = ('*/*', )

        acceptable_media_type_lut = _build_acceptable_media_type_lut(
            media_types,
            supported_media_type_lut
        )

        # Check whether the expected media is specified as one of the
        # acceptable media types.
        expected_media_type = transfer_syntax_uid_lut[transfer_syntax_uid]
        found_matching_media_type = False
        if _are_frames_encapsulated(transfer_syntax_uid):
            wildcards = {'*/*', '*/', 'image/*', 'image/'}
            if any([w in acceptable_media_type_lut for w in wildcards]):
                found_matching_media_type = True
        else:
            wildcards = {'*/*', '*/', 'application/*', 'application/'}
            if any([w in acceptable_media_type_lut for w in wildcards]):
                found_matching_media_type = True

        if expected_media_type in acceptable_media_type_lut:
            found_matching_media_type = True
            # If expected media type is specified as one of the acceptable
            # media types, check whether the corresponding transfer syntax is
            # appropriate.
            expected_transfer_syntaxes = acceptable_media_type_lut[
                expected_media_type
            ]
            if (
                transfer_syntax_uid not in expected_transfer_syntaxes and
                '*' not in expected_transfer_syntaxes
            ):
                raise _create_client_error(
                    method='GET',
                    url=url,
                    reason=(
                        'Instance frames cannot be retrieved using media type '
                        f'"{expected_media_type}" '
                        'with any of the specified transfer syntaxes: '
                        '"{}".'.format(
                            '", "'.join(expected_transfer_syntaxes)
                        )
                    ),
                    status_code=406
                )

        if found_matching_media_type:
            image_type = None
        else:
            # If expected media is not specified as one of the acceptable media
            # types, check whether one of the acceptable media types is
            # suitable for lossless recompression of the frame.
            if (
                'image/jp2' in acceptable_media_type_lut and
                (
                    '1.2.840.10008.1.2.4.90'
                    in acceptable_media_type_lut['image/jp2']
                )
            ):
                image_type = 'image/jp2'
            else:
                raise _create_client_error(
                    method='GET',
                    url=url,
                    reason=(
                        'Instance frames cannot be retrieved using any of the '
                        f'acceptable media types: {media_types}.'
                    ),
                    status_code=406
                )

        return image_type

    def iter_instance_frames(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        frame_numbers: List[int],
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None
    ) -> Iterator[bytes]:
        """Iterate over frames of an image instance.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        sop_instance_uid: str
            SOP Instance UID
        frame_numbers: List[int]
            Frame numbers
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        Iterator[bytes]
            Frames

        """  # noqa: E501
        logger.info(
            f'iterate over frames of instance "{sop_instance_uid}" of '
            f'series "{series_instance_uid}" of study "{study_instance_uid}"'
        )
        if len(frame_numbers) == 0:
            raise ValueError('At least one frame number must be provided.')

        url = self._get_instances_url(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid
        )
        url += '/frames'
        try:
            file_path = self._db_manager.get_instance_file_path(
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
            )

            metadata, transfer_syntax_uid, \
                first_frame_offset, bot = self._db_manager.get_instance_info(
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                    sop_instance_uid=sop_instance_uid
                )
            reencoding_media_type = self._check_media_types_for_instance_frames(
                url,
                transfer_syntax_uid,
                media_types
            )
            requires_reencoding = False
            if (
                reencoding_media_type is not None and
                reencoding_media_type != 'application/octet-stream'
               ):
                requires_reencoding = True

            image_file_pointer = self._get_image_file_pointer(
                file_path,
                transfer_syntax_uid=transfer_syntax_uid
            )

            for frame_number in frame_numbers:
                if frame_number > int(getattr(metadata, 'NumberOfFrames', '1')):
                    raise ValueError(
                        f'Provided frame number {frame_number} exceeds number '
                        'of available frames.'
                    )
                frame_index = frame_number - 1
                frame = _read_frame(
                    image_file_pointer,
                    first_frame_offset=first_frame_offset,
                    basic_offset_table=bot,
                    frame_index=frame_index,
                    transfer_syntax_uid=transfer_syntax_uid
                )
                if requires_reencoding:
                    array = _decode_frame(
                        frame=frame,
                        frame_index=frame_index,
                        transfer_syntax_uid=transfer_syntax_uid,
                        rows=metadata.Rows,
                        columns=metadata.Columns,
                        samples_per_pixel=metadata.SamplesPerPixel,
                        bits_allocated=metadata.BitsAllocated,
                        bits_stored=metadata.BitsStored,
                        photometric_interpretation=getattr(
                            metadata,
                            'PhotometricInterpretation'
                        ),
                        pixel_representation=metadata.PixelRepresentation,
                        planar_configuration=getattr(
                            metadata,
                            'PlanarConfiguration',
                            None
                        )
                    )
                    if reencoding_media_type == 'image/jp2':
                        image_type = 'jpeg2000'
                        image_kwargs = {'irreversible': False}
                        image = Image.fromarray(array)
                        with io.BytesIO() as fp:
                            image.save(
                                fp,
                                image_type,
                                **image_kwargs   # type: ignore
                            )
                            reencoded_frame = fp.getvalue()
                        yield reencoded_frame
                    else:
                        raise _create_client_error(
                            method='GET',
                            url=url,
                            reason=(
                                'Cannot retrieve frames using media type '
                                f'"{reencoding_media_type}".'
                            ),
                            status_code=406
                        )
                else:
                    yield frame

            image_file_pointer.close()

        except requests.HTTPError:
            raise
        except Exception as error:
            raise _create_server_error(
                method='GET',
                url=url,
                reason=str(error)
            )

    def retrieve_instance_frames(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        frame_numbers: List[int],
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None
    ) -> List[bytes]:
        """Retrieve one or more frames of an image instance.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        sop_instance_uid: str
            SOP Instance UID
        frame_numbers: List[int]
            Frame numbers
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        List[bytes]
            Frames

        """  # noqa: E501
        logger.info(
            f'retrieve frames of instance "{sop_instance_uid}" of '
            f'series "{series_instance_uid}" of study "{study_instance_uid}"'
        )
        if len(frame_numbers) == 0:
            raise ValueError('At least one frame number must be provided.')

        url = self._get_instances_url(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid
        )
        url += '/frames/{}'.format(','.join([str(n) for n in frame_numbers]))
        try:
            file_path = self._db_manager.get_instance_file_path(
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
            )
            metadata, transfer_syntax_uid, \
                first_frame_offset, bot = self._db_manager.get_instance_info(
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                    sop_instance_uid=sop_instance_uid
                )
            reencoding_media_type = self._check_media_types_for_instance_frames(
                url,
                transfer_syntax_uid,
                media_types
            )
            requires_reencoding = False
            if (
                reencoding_media_type is not None and
                reencoding_media_type != 'application/octet-stream'
               ):
                requires_reencoding = True

            image_file_pointer = self._get_image_file_pointer(
                file_path,
                transfer_syntax_uid=transfer_syntax_uid
            )

            # Check all indices first before attempting to read the first frame.
            frame_indices = []
            for frame_number in frame_numbers:
                if frame_number > int(getattr(metadata, 'NumberOfFrames', '1')):
                    raise ValueError(
                        f'Provided frame number {frame_number} exceeds number '
                        'of available frames.'
                    )
                frame_index = frame_number - 1
                frame_indices.append(frame_index)

            retrieved_frames = []
            for frame_index in frame_indices:
                frame = _read_frame(
                    image_file_pointer,
                    first_frame_offset=first_frame_offset,
                    basic_offset_table=bot,
                    frame_index=frame_index,
                    transfer_syntax_uid=transfer_syntax_uid
                )
                if requires_reencoding:
                    array = _decode_frame(
                        frame=frame,
                        frame_index=frame_index,
                        transfer_syntax_uid=transfer_syntax_uid,
                        rows=metadata.Rows,
                        columns=metadata.Columns,
                        samples_per_pixel=metadata.SamplesPerPixel,
                        bits_allocated=metadata.BitsAllocated,
                        bits_stored=metadata.BitsStored,
                        photometric_interpretation=getattr(
                            metadata,
                            'PhotometricInterpretation'
                        ),
                        pixel_representation=metadata.PixelRepresentation,
                        planar_configuration=getattr(
                            metadata,
                            'PlanarConfiguration',
                            None
                        )
                    )
                    if reencoding_media_type == 'image/jp2':
                        image_type = 'jpeg2000'
                        image_kwargs = {'irreversible': False}
                        image = Image.fromarray(array)
                        with io.BytesIO() as fp:
                            image.save(
                                fp,
                                image_type,
                                **image_kwargs   # type: ignore
                            )
                            reencoded_frame = fp.getvalue()
                    else:
                        raise ValueError(
                            'Cannot retrieve frames using media type '
                            f'"{reencoding_media_type}".'
                        )
                    retrieved_frames.append(reencoded_frame)
                else:
                    retrieved_frames.append(frame)

            image_file_pointer.close()
            return retrieved_frames

        except requests.HTTPError:
            raise
        except Exception as error:
            raise _create_server_error(
                method='GET',
                url=url,
                reason=str(error)
            )

    def retrieve_instance_frames_rendered(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        frame_numbers: List[int],
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None,
        params: Optional[Dict[str, str]] = None,
    ) -> bytes:
        """Retrieve server-side rendered frames of an image instance.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        sop_instance_uid: str
            SOP Instance UID
        frame_numbers: List[int]
            Frame numbers
        media_types: Union[Tuple[Union[str, Tuple[str, str]], ...], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        params: Union[Dict[str, str], None], optional
            Additional query parameters

        Returns
        -------
        bytes
            Rendered representation of frames

        """  # noqa: E501
        logger.info(
            f'retrieve rendered frames of instance "{sop_instance_uid}" of '
            f'series "{series_instance_uid}" of study "{study_instance_uid}"'
        )
        if len(frame_numbers) == 0:
            raise ValueError('A frame number must be provided.')
        elif len(frame_numbers) > 1:
            raise ValueError(
                'Only rendering of a single frame is supported for now.'
            )
        frame_number = frame_numbers[0]
        frame_index = frame_number - 1

        url = self._get_instances_url(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid
        )
        url += f'/frames/{frame_number}/rendered'
        if params is not None:
            url += build_query_string(params)
        try:
            file_path = self._db_manager.get_instance_file_path(
                study_instance_uid,
                series_instance_uid,
                sop_instance_uid,
            )
            metadata, transfer_syntax_uid, \
                first_frame_offset, bot = self._db_manager.get_instance_info(
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                    sop_instance_uid=sop_instance_uid
                )
            image_file_pointer = self._get_image_file_pointer(
                file_path,
                transfer_syntax_uid=transfer_syntax_uid
            )
            frame = _read_frame(
                image_file_pointer,
                first_frame_offset=first_frame_offset,
                basic_offset_table=bot,
                frame_index=frame_index,
                transfer_syntax_uid=transfer_syntax_uid
            )
            image_file_pointer.close()

            # TODO: ICC Profile
            codec_name, codec_kwargs = self._get_image_codec_parameters(
                transfer_syntax_uid=transfer_syntax_uid,
                media_types=media_types,
                params=params
            )

            if codec_name is None:
                return frame

            array = _decode_frame(
                frame=frame,
                frame_index=frame_index,
                transfer_syntax_uid=transfer_syntax_uid,
                rows=metadata.Rows,
                columns=metadata.Columns,
                samples_per_pixel=metadata.SamplesPerPixel,
                bits_allocated=metadata.BitsAllocated,
                bits_stored=metadata.BitsStored,
                photometric_interpretation=metadata.PhotometricInterpretation,
                pixel_representation=metadata.PixelRepresentation,
                planar_configuration=getattr(
                    metadata,
                    'PlanarConfiguration',
                    None
                )
            )
            image = Image.fromarray(array)
            with io.BytesIO() as fp:
                image.save(fp, codec_name, **codec_kwargs)  # type: ignore
                fp.seek(0)
                reencoded_frame = fp.read()
            return reencoded_frame

        except requests.HTTPError:
            raise
        except Exception as error:
            raise _create_server_error(
                method='GET',
                url=url,
                reason=str(error)
            )

    def _get_image_codec_parameters(
        self,
        transfer_syntax_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None,
        params: Optional[Dict[str, str]] = None,
        icc_profile: Optional[bytes] = None
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        if media_types is not None:
            acceptable_media_types = list(set([
                m[0]
                if isinstance(m, tuple)
                else m
                for m in media_types
            ]))
            are_media_types_valid = all(
                m.startswith('image')
                for m in acceptable_media_types
            )
            if not are_media_types_valid:
                raise ValueError(
                    'Compressed instance frames can only be retrieved in '
                    'rendered format using media type "image".'
                )
            if 'image/png' in acceptable_media_types:
                image_type = 'png'
            elif 'image/jp2' in acceptable_media_types:
                if transfer_syntax_uid == '1.2.840.10008.1.2.4.90':
                    image_type = None
                else:
                    # Lossless recompression
                    image_type = 'jpeg2000'
            elif 'image/jpeg' in acceptable_media_types:
                if transfer_syntax_uid == '1.2.840.10008.1.2.4.50':
                    # Avoid lossy recompression of lossy compressed frames.
                    image_type = None
                else:
                    # Allow lossy recompression in case of retrieve rendered.
                    logger.warning(
                        'frames of instance "{sop_instance_uid}" are lossy '
                        'recompressed upon retrieval'
                    )
                    image_type = 'jpeg'
            else:
                raise ValueError(
                    'Cannot retrieve frames of instance in rendered '
                    'format using any of the acceptable media types: '
                    '"{}".'.format('", "'.join(acceptable_media_types))
                )
        else:
            if transfer_syntax_uid == '1.2.840.10008.1.2.4.50':
                # Avoid lossy recompression of lossy compressed frames.
                image_type = None
            else:
                image_type = 'jpeg'

        image_kwargs: Dict[str, Any] = {
            # Avoid re-compression when encoding in PNG format
            'png': {'compress_level': 0, 'optimize': False},
            'jpeg': {'quality': 100, 'optimize': False},
            'jpeg2000': {'irreversible': False},
        }
        if params is not None and image_type is not None:
            include_icc_profile = params.get('icc_profile', 'no')
            if include_icc_profile == 'yes':
                if icc_profile is None:
                    icc_profile = createProfile('sRGB')
                image_kwargs[image_type]['icc_profile'] = ImageCmsProfile(
                    icc_profile
                )
            elif include_icc_profile == 'srgb':
                icc_profile = createProfile('sRGB')
                image_kwargs[image_type]['icc_profile'] = ImageCmsProfile(
                    icc_profile
                )
            elif include_icc_profile == 'no':
                pass
            else:
                raise ValueError(
                    f'ICC Profile "{include_icc_profile}" is not supported.'
                )

        if image_type is None:
            return (image_type, {})

        return (image_type, image_kwargs[image_type])

    @staticmethod
    def lookup_keyword(
        tag: Union[int, str, Tuple[int, int], BaseTag]
    ) -> str:
        """Look up the keyword of a DICOM attribute.

        Parameters
        ----------
        tag: Union[str, int, Tuple[int, int], pydicom.tag.BaseTag]
            Attribute tag (e.g. ``"00080018"``)

        Returns
        -------
        str
            Attribute keyword (e.g. ``"SOPInstanceUID"``)

        """
        keyword = keyword_for_tag(tag)
        if keyword is None:
            raise KeyError(f'Could not find a keyword for tag {tag}.')
        return keyword

    @staticmethod
    def lookup_tag(keyword: str) -> str:
        """Look up the tag of a DICOM attribute.

        Parameters
        ----------
        keyword: str
            Attribute keyword (e.g. ``"SOPInstanceUID"``)

        Returns
        -------
        str
            Attribute tag as HEX string (e.g. ``"00080018"``)

        """
        tag = tag_for_keyword(keyword)
        if tag is None:
            raise KeyError(f'Could not find a tag for "{keyword}".')
        tag = Tag(tag)
        return '{0:04x}{1:04x}'.format(tag.group, tag.element).upper()

    def store_instances(
        self,
        datasets: Sequence[Dataset],
        study_instance_uid: Optional[str] = None
    ) -> Dataset:
        """Store instances.

        Parameters
        ----------
        datasets: Sequence[pydicom.dataset.Dataset]
            Instances that should be stored
        study_instance_uid: Union[str, None], optional
            Study Instance UID

        Returns
        -------
        pydicom.dataset.Dataset
            Information about status of stored instances

        """
        message = 'store instances'
        if study_instance_uid is not None:
            message += f' of study "{study_instance_uid}"'
        logger.info(message)

        for ds in datasets:
            if not isinstance(ds, Dataset):
                raise TypeError(
                    'Items of argument "datasets" must have type '
                    'pydicom.dataset.Dataset.'
                )
            if not hasattr(ds, 'file_meta'):
                raise ValueError(
                    'Datasets must contain File Meta Information.'
                )

        url = self._get_instances_url(study_instance_uid)
        if self._readonly:
            raise _create_client_error(
                method='POST',
                url=url,
                reason='Storage of instances is not allowed.',
                status_code=403
            )
        try:
            successes, failures = self._db_manager.insert_instances(datasets)

            response = Dataset()
            response.RetrieveURL = None  # type 2

            if len(successes) == 0 and len(failures) == 0:
                raise RuntimeError('Failed to store instances.')

            if len(successes) > 0:
                response.ReferencedSOPSequence = []
                for ds, file_path, file_content in successes:
                    directory = file_path.parent
                    directory.mkdir(exist_ok=True, parents=True)
                    with open(file_path, 'wb') as fp:
                        fp.write(file_content)

                    success_item = Dataset()
                    success_item.ReferencedSOPClassUID = ds.SOPClassUID
                    success_item.ReferencedSOPInstanceUID = ds.SOPInstanceUID
                    success_item.RetrieveURL = None  # type 2
                    response.ReferencedSOPSequence.append(success_item)

            if len(failures) > 0:
                response.FailedSOPSequence = []
                for ds in failures:
                    failure_item = Dataset()
                    failure_item.FailureReason = 272
                    failure_item.ReferencedSOPClassUID = ds.SOPClassUID
                    failure_item.ReferencedSOPInstanceUID = ds.SOPInstanceUID
                    response.FailedSOPSequence.append(failure_item)

            return response

        except requests.HTTPError:
            raise
        except Exception as error:
            url = self._get_studies_url(study_instance_uid)
            raise _create_server_error(
                method='POST',
                url=url,
                reason=str(error)
            )

    def delete_study(self, study_instance_uid: str) -> None:
        """Delete all instances of a study.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID

        """
        if study_instance_uid is None:
            raise ValueError(
              'Study Instance UID is required for deletion of a study.'
            )
        url = self._get_studies_url(study_instance_uid)
        if self._readonly:
            raise _create_client_error(
                method='DELETE',
                url=url,
                reason='Deletion of study is not allowed.',
                status_code=403
            )
        try:
            for uids in self._db_manager.get_instance_identifiers(
                study_instance_uid=study_instance_uid
            ):
                self.delete_instance(*uids)
        except Exception as error:
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def delete_series(
        self,
        study_instance_uid: str,
        series_instance_uid: str
    ) -> None:
        """Delete all instances of a series.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID

        """
        if study_instance_uid is None:
            raise ValueError(
              'Study Instance UID is required for deletion of a series.'
            )
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for deletion of a series.'
            )
        url = self._get_series_url(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
        )
        if self._readonly:
            raise _create_client_error(
                method='DELETE',
                url=url,
                reason='Deletion of series is not allowed.',
                status_code=403
            )
        try:
            for uids in self._db_manager.get_instance_identifiers(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
            ):
                self.delete_instance(*uids)
        except requests.HTTPError:
            raise
        except Exception as error:
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )

    def delete_instance(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str
    ) -> None:
        """Delete specified instance.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        sop_instance_uid: str
            SOP Instance UID

        """
        if study_instance_uid is None:
            raise ValueError(
              'Study Instance UID is required for deletion of an instance.'
            )
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for deletion of an instance.'
            )
        if sop_instance_uid is None:
            raise ValueError(
                'SOP Instance UID is required for deletion of an instance.'
            )
        url = self._get_instances_url(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid
        )
        if self._readonly:
            raise _create_client_error(
                method='DELETE',
                url=url,
                reason='Deletion of instance is not allowed.',
                status_code=403
            )
        try:
            file_path = self._db_manager.get_instance_file_path(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                sop_instance_uid=sop_instance_uid,
            )
            self._db_manager.delete_instances(
                uids=[
                    (study_instance_uid, series_instance_uid, sop_instance_uid)
                ]
            )
            os.remove(file_path)
        except Exception as error:
            raise _create_server_error(
                method='DELETE',
                url=url,
                reason=str(error)
            )
