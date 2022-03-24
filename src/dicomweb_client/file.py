"""Client to access DICOM Part10 files through a layer of abstraction."""
import collections
import io
import logging
import math
import os
import re
import sqlite3
import sys
import time
import traceback
from collections import OrderedDict
from enum import Enum
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import (
    Any,
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

import numpy as np
from PIL import Image
from PIL.ImageCms import ImageCmsProfile, createProfile
from pydicom.dataset import Dataset, FileMetaDataset
from pydicom.encaps import encapsulate, get_frame_offsets
from pydicom.errors import InvalidDicomError
from pydicom.filebase import DicomFileLike
from pydicom.datadict import dictionary_VR, keyword_for_tag, tag_for_keyword
from pydicom.filereader import (
    data_element_offset_to_value,
    dcmread,
    read_file_meta_info,
    read_partial,
)
from pydicom.filewriter import dcmwrite
from pydicom.pixel_data_handlers.numpy_handler import unpack_bits
from pydicom.tag import (
    BaseTag,
    ItemTag,
    SequenceDelimiterTag,
    Tag,
    TupleTag,
)
from pydicom.uid import UID
from pydicom.valuerep import DA, DT, TM

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


def _get_bot(fp: DicomFileLike, number_of_frames: int) -> List[int]:
    """Read or build the Basic Offset Table (BOT).

    Parameters
    ----------
    fp: pydicom.filebase.DicomFileLike
        Pointer for DICOM PS3.10 file stream positioned at the first byte of
        the Pixel Data element
    number_of_frames: int
        Number of frames contained in the Pixel Data element

    Returns
    -------
    List[int]
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
            number_of_frames=number_of_frames
        )

    return basic_offset_table


def _read_bot(fp: DicomFileLike) -> List[int]:
    """Read the Basic Offset Table (BOT) of an encapsulated Pixel Data element.

    Parameters
    ----------
    fp: pydicom.filebase.DicomFileLike
        Pointer for DICOM PS3.10 file stream positioned at the first byte of
        the Pixel Data element

    Returns
    -------
    List[int]
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
    return offsets


def _build_bot(fp: DicomFileLike, number_of_frames: int) -> List[int]:
    """Build a Basic Offset Table (BOT) for an encapsulated Pixel Data element.

    Parameters
    ----------
    fp: pydicom.filebase.DicomFileLike
        Pointer for DICOM PS3.10 file stream positioned at the first byte of
        the Pixel Data element following the empty Basic Offset Table (BOT)
    number_of_frames: int
        Total number of frames in the dataset

    Returns
    -------
    List[int]
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

        # In case of fragmentation, we only want to get the offsets to the
        # first fragment of a given frame. We can identify those based on the
        # JPEG and JPEG 2000 markers that should be found at the beginning and
        # end of the compressed byte stream.
        if first_two_bytes in _START_MARKERS:
            current_offset = frame_position - initial_position
            offset_values.append(current_offset)

        i += 1
        fp.seek(length - 2, 1)  # minus the first two bytes

    if len(offset_values) != number_of_frames:
        raise ValueError(
            'Number of frame items does not match specified Number of Frames.'
        )
    else:
        basic_offset_table = offset_values

    fp.seek(initial_position, 0)
    return basic_offset_table


class _ImageFileReader:

    """Class for reading DICOM files that represent Image Information Entities.

    The class provides methods for efficient access to individual Frame items
    contained in the Pixel Data element of a Data Set stored in a Part10 file
    on disk without loading the entire element into memory.

    """

    def __init__(self, fp: Union[str, Path, DicomFileLike]):
        """
        Parameters
        ----------
        fp: Union[str, pathlib.Path, pydicom.filebase.DicomfileLike]
            DICOM Part10 file containing a dataset of an image SOP Instance

        """
        self._filepointer: Union[DicomFileLike, None]
        self._filepath: Union[Path, None]
        if isinstance(fp, DicomFileLike):
            is_little_endian, is_implicit_VR = self._check_file_format(fp)
            try:
                if fp.is_little_endian != is_little_endian:
                    raise ValueError(
                        'Transfer syntax of file object has incorrect value '
                        'for attribute "is_little_endian".'
                    )
            except AttributeError:
                raise AttributeError(
                    'Transfer syntax of file object does not have '
                    'attribute "is_little_endian".'
                )
            try:
                if fp.is_implicit_VR != is_implicit_VR:
                    raise ValueError(
                        'Transfer syntax of file object has incorrect value '
                        'for attribute "is_implicit_VR".'
                    )
            except AttributeError:
                raise AttributeError(
                    'Transfer syntax of file object does not have '
                    'attribute "is_implicit_VR".'
                )
            self._filepointer = fp
            self._filepath = None
        elif isinstance(fp, (str, Path)):
            self._filepath = Path(fp)
            self._filepointer = None
        else:
            raise TypeError(
                'Argument "filename" must either an open DICOM file object or '
                'the path to a DICOM file stored on disk.'
            )

        # We use threads to read multiple frames in parallel. Since the
        # operation is I/O rather CPU limited, we don't need to worry about
        # the Global Interpreter Lock (GIL) and use lighweight threads
        # instead of separate processes. However, that won't be as useful for
        # decoding, since that operation is CPU limited.
        self._pool: Union[ThreadPool, None] = None

        self._metadata: Dataset = Dataset()
        self._is_open = False
        self._as_float = False
        self._bytes_per_frame_uncompressed: int = -1
        self._basic_offset_table: List[int] = []
        self._first_frame_offset: int = -1
        self._pixel_data_offset: int = -1
        self._pixels_per_frame: int = -1

    def _check_file_format(self, fp: DicomFileLike) -> Tuple[bool, bool]:
        """Check whether file object represents a DICOM Part 10 file.

        Parameters
        ----------
        fp: pydicom.filebase.DicomFileLike
            DICOM file object

        Returns
        -------
        is_little_endian: bool
            Whether the data set is encoded in little endian transfer syntax
        is_implicit_VR: bool
            Whether value representations of data elements in the data set
            are implicit

        Raises
        ------
        InvalidDicomError
            If the file object does not represent a DICOM Part 10 file

        """
        def is_main_tag(tag: BaseTag, VR: Optional[str], length: int) -> bool:
            return tag >= 0x00040000

        pos = fp.tell()
        ds = read_partial(fp, stop_when=is_main_tag)  # type: ignore
        fp.seek(pos)
        transfer_syntax_uid = UID(ds.file_meta.TransferSyntaxUID)
        return (
            transfer_syntax_uid.is_little_endian,
            transfer_syntax_uid.is_implicit_VR,
        )

    def __enter__(self) -> '_ImageFileReader':
        self.open()
        return self

    def __exit__(self, except_type, except_value, except_trace) -> None:
        self._fp.close()
        if except_value:
            sys.stdout.write(
                'Error while accessing file "{}":\n{}'.format(
                    self._filepath, str(except_value)
                )
            )
            for tb in traceback.format_tb(except_trace):
                sys.stdout.write(tb)
            raise

    @property
    def _fp(self) -> DicomFileLike:
        if self._filepointer is None:
            raise IOError('File has not been opened for reading.')
        return self._filepointer

    def open(self) -> None:
        """Open file for reading.

        Raises
        ------
        FileNotFoundError
            When file cannot be found
        OSError
            When file cannot be opened
        IOError
            When DICOM metadata cannot be read from file
        ValueError
            When DICOM dataset contained in file does not represent an image

        Note
        ----
        Reads the metadata of the DICOM Data Set contained in the file and
        builds a Basic Offset Table to speed up subsequent frame-level access.

        """
        # This methods sets several attributes on the object, which cannot
        # (or should not) be set in the constructor. Other methods assert that
        # this method has been called first by checking the value of the
        # "_is_open" attribute.
        if self._is_open:
            return

        if self._filepointer is None:
            # This should not happen is just for mypy to be happy
            if self._filepath is None:
                raise ValueError(f'File not found: "{self._filepath}".')
            logger.debug('read File Meta Information')
            try:
                file_meta = read_file_meta_info(self._filepath)
            except FileNotFoundError:
                raise ValueError('No file path was set.')
            except InvalidDicomError:
                raise InvalidDicomError(
                    f'File is not a valid DICOM file: "{self._filepath}".'
                )
            except Exception:
                raise IOError(f'Could not read file: "{self._filepath}".')

            transfer_syntax_uid = UID(file_meta.TransferSyntaxUID)
            if transfer_syntax_uid is None:
                raise IOError(
                    'File is not a valid DICOM file: "{self._filepath}".'
                    'It lacks File Meta Information.'
                )
            self._transfer_syntax_uid: UID = transfer_syntax_uid
            is_little_endian = transfer_syntax_uid.is_little_endian
            is_implicit_VR = transfer_syntax_uid.is_implicit_VR
            self._filepointer = DicomFileLike(open(self._filepath, 'rb'))
            self._filepointer.is_little_endian = is_little_endian
            self._filepointer.is_implicit_VR = is_implicit_VR

        logger.debug('read metadata elements')
        try:
            tmp = dcmread(self._fp, stop_before_pixels=True)
        except Exception as error:
            raise IOError(
                f'DICOM metadata cannot be read from file: "{error}"'
            )

        # Construct a new Dataset that is fully decoupled from the file,
        # i.e., that does not contain any File Meta Information
        del tmp.file_meta
        self._metadata = Dataset(tmp)

        self._pixels_per_frame = int(np.product([
            self._metadata.Rows,
            self._metadata.Columns,
            self._metadata.SamplesPerPixel
        ]))

        self._pixel_data_offset = self._fp.tell()
        # Determine whether dataset contains a Pixel Data element
        try:
            tag = TupleTag(self._fp.read_tag())
        except EOFError:
            raise ValueError(
                'Dataset does not represent an image information entity.'
            )
        if int(tag) not in _PIXEL_DATA_TAGS:
            raise ValueError(
                'Dataset does not represent an image information entity.'
            )
        self._as_float = False
        if int(tag) in _FLOAT_PIXEL_DATA_TAGS:
            self._as_float = True

        # Reset the file pointer to the beginning of the Pixel Data element
        self._fp.seek(self._pixel_data_offset, 0)

        logger.debug('build Basic Offset Table')
        try:
            number_of_frames = int(self._metadata.NumberOfFrames)
        except AttributeError:
            number_of_frames = 1
        if self._transfer_syntax_uid.is_encapsulated:
            try:
                self._basic_offset_table = _get_bot(
                    self._fp,
                    number_of_frames=number_of_frames
                )
            except Exception as error:
                raise IOError(
                    f'Failed to build Basic Offset Table: "{error}"'
                )
            self._first_frame_offset = self._fp.tell()
        else:
            if self._fp.is_implicit_VR:
                header_offset = 4 + 4  # tag and length
            else:
                header_offset = 4 + 2 + 2 + 4  # tag, VR, reserved, and length
            self._first_frame_offset = self._pixel_data_offset + header_offset
            n_pixels = self._pixels_per_frame
            bits_allocated = self._metadata.BitsAllocated
            if bits_allocated == 1:
                # Determine the nearest whole number of bytes needed to contain
                # 1-bit pixel data. e.g. 10 x 10 1-bit pixels is 100 bits,
                # which are packed into 12.5 -> 13 bytes
                self._bytes_per_frame_uncompressed = (
                    n_pixels // 8 + (n_pixels % 8 > 0)
                )
                self._basic_offset_table = [
                    int(math.floor(i * n_pixels / 8))
                    for i in range(number_of_frames)
                ]
            else:
                self._bytes_per_frame_uncompressed = (
                    n_pixels * bits_allocated // 8
                )
                self._basic_offset_table = [
                    i * self._bytes_per_frame_uncompressed
                    for i in range(number_of_frames)
                ]

        if len(self._basic_offset_table) != number_of_frames:
            raise ValueError(
                'Length of Basic Offset Table does not match '
                'Number of Frames.'
            )

        self._pool = ThreadPool()
        self._is_open = True

    def _assert_is_open(self) -> None:
        if not self._is_open:
            raise IOError('DICOM image file has not been opened for reading.')

    @property
    def transfer_syntax_uid(self) -> UID:
        """pydicom.uid.UID: Transfer Syntax UID"""
        self._assert_is_open()
        return self._transfer_syntax_uid

    @property
    def metadata(self) -> Dataset:
        """pydicom.dataset.Dataset: Metadata"""
        self._assert_is_open()
        return self._metadata

    def close(self) -> None:
        """Close file."""
        if self._pool is not None:
            self._pool.close()
            self._pool.terminate()
        if self._fp is not None:
            self._fp.close()
        self._is_open = False

    def read_frames(
        self,
        indices: Iterable[int],
        parallel: bool = False
    ) -> List[bytes]:
        """Read the pixel data of one or more frame items.

        Parameters
        ----------
        indices: Iterable[int]
            Zero-based frame indices
        parallel: bool, optional
            Whether frame items should be read in parallel using multiple
            threads

        Returns
        -------
        List[bytes]
            Pixel data of frame items encoded in the transfer syntax.

        Raises
        ------
        IOError
            When frames could not be read

        """
        self._assert_is_open()
        if parallel:
            func = self.read_frame
            return self._pool.map(func, indices)  # type: ignore
        else:
            return [self.read_frame(i) for i in indices]

    def read_frame(self, index: int) -> bytes:
        """Read the pixel data of an individual frame item.

        Parameters
        ----------
        index: int
            Zero-based frame index

        Returns
        -------
        bytes
            Pixel data of a given frame item encoded in the transfer syntax.

        Raises
        ------
        IOError
            When frame could not be read

        """
        self._assert_is_open()
        if index > self.number_of_frames:
            raise ValueError(
                f'Frame index {index} exceeds number of frames in image: '
                f'{self.number_of_frames}.'
            )

        logger.debug(f'read frame #{index}')

        frame_offset = self._basic_offset_table[index]
        self._fp.seek(self._first_frame_offset + frame_offset, 0)
        if self._transfer_syntax_uid.is_encapsulated:
            try:
                stop_at = self._basic_offset_table[index + 1] - frame_offset
            except IndexError:
                # For the last frame, there is no next offset available.
                stop_at = -1
            n = 0
            # A frame may consist of multiple items (fragments).
            fragments = []
            while True:
                tag = TupleTag(self._fp.read_tag())
                if n == stop_at or int(tag) == SequenceDelimiterTag:
                    break
                if int(tag) != ItemTag:
                    raise ValueError(f'Failed to read frame #{index}.')
                length = self._fp.read_UL()
                fragments.append(self._fp.read(length))
                n += 4 + 4 + length
            frame_data = b''.join(fragments)
        else:
            frame_data = self._fp.read(self._bytes_per_frame_uncompressed)

        if len(frame_data) == 0:
            raise IOError(f'Failed to read frame #{index}.')

        return frame_data

    def decode_frame(self, index: int, value: bytes):
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
        self._assert_is_open()
        logger.debug(f'decode frame #{index}')

        metadata = self.metadata
        if metadata.BitsAllocated == 1:
            unpacked_frame = unpack_bits(value)
            rows, columns = metadata.Rows, self.metadata.Columns
            n_pixels = self._pixels_per_frame
            pixel_offset = int(((index * n_pixels / 8) % 1) * 8)
            pixel_array = unpacked_frame[pixel_offset:pixel_offset + n_pixels]
            return pixel_array.reshape(rows, columns)
        else:
            # This hack creates a small dataset containing a Pixel Data element
            # with only a single frame item, which can then be decoded using the
            # existing pydicom API.
            ds = Dataset()
            ds.file_meta = FileMetaDataset()
            ds.file_meta.TransferSyntaxUID = self._transfer_syntax_uid
            ds.Rows = metadata.Rows
            ds.Columns = metadata.Columns
            ds.SamplesPerPixel = metadata.SamplesPerPixel
            ds.PhotometricInterpretation = metadata.PhotometricInterpretation
            ds.PixelRepresentation = metadata.PixelRepresentation
            ds.PlanarConfiguration = metadata.get('PlanarConfiguration', None)
            ds.BitsAllocated = metadata.BitsAllocated
            ds.BitsStored = metadata.BitsStored
            ds.HighBit = metadata.HighBit
            if self._transfer_syntax_uid.is_encapsulated:
                ds.PixelData = encapsulate(frames=[value])
            else:
                ds.PixelData = value
            return ds.pixel_array

    def read_and_decode_frames(
        self,
        indices: Iterable[int],
        parallel: bool = False
    ) -> List[np.ndarray]:
        """Read and decode the pixel data of one or more frame items.

        Parameters
        ----------
        indices: Iterable[int]
            Zero-based frame indices
        parallel: bool, optional
            Whether frame items should be read in parallel using multiple
            threads

        Returns
        -------
        List[numpy.ndarray]
            Pixel arrays of frame items

        Raises
        ------
        IOError
            When frames could not be read

        """
        self._assert_is_open()
        if parallel:
            func = self.read_and_decode_frame
            return self._pool.map(func, indices)  # type: ignore
        else:
            return [self.read_and_decode_frame(i) for i in indices]

    def read_and_decode_frame(self, index: int):
        """Read and decode the pixel data of an individual frame item.

        Parameters
        ----------
        index: int
            Zero-based frame index

        Returns
        -------
        numpy.ndarray
            Array of decoded pixels of the frame with shape (Rows x Columns)
            in case of a monochrome image or (Rows x Columns x SamplesPerPixel)
            in case of a color image.

        Raises
        ------
        IOError
            When frame could not be read

        """
        frame = self.read_frame(index)
        return self.decode_frame(index, frame)

    @property
    def number_of_frames(self) -> int:
        """int: Number of frames"""
        self._assert_is_open()
        try:
            return int(self.metadata.NumberOfFrames)
        except AttributeError:
            return 1


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


class DICOMfileClient:

    """Client for managing DICOM Part10 files in a DICOMweb-like manner.

    Facilitates serverless access to data stored locally on a file system as
    DICOM Part10 files.

    Note
    ----
    The class exposes the same :class:`dicomweb_client.api.DICOMClient`
    interface as the :class:`dicomweb_client.api.DICOMwebClient` class.
    While method parameters and return values have the same types, but the
    types of exceptions may differ.

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

    """

    def __init__(
        self,
        base_dir: Union[Path, str],
        update_db: bool = False,
        recreate_db: bool = False,
        in_memory: bool = False
    ):
        """Instantiate client.

        Parameters
        ----------
        base_dir: Union[pathlib.Path, str]
            Path to base directory containing DICOM files
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
        self.base_dir = Path(base_dir).resolve()
        if in_memory:
            filename = ':memory:'
        else:
            filename = '.dicom-file-client.db'
        self._db_filepath = self.base_dir.joinpath(filename)
        if not self._db_filepath.exists():
            update_db = True

        self._db_connection_handle: Union[sqlite3.Connection, None] = None
        self._db_cursor_handle: Union[sqlite3.Cursor, None] = None
        if recreate_db:
            self._drop_db()
            update_db = True

        self._create_db()

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

        self._reader_cache: OrderedDict[Path, _ImageFileReader] = OrderedDict()
        self._max_reader_cache_size = 50

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
            for image_file_reader in self._reader_cache.values():
                image_file_reader.close()
        finally:
            contents['_db_connection_handle'] = None
            contents['_db_cursor_handle'] = None
            contents['_reader_cache'] = OrderedDict()
        return contents

    @property
    def _connection(self) -> sqlite3.Connection:
        """sqlite3.Connection: database connection"""
        if self._db_connection_handle is None:
            self._db_connection_handle = sqlite3.connect(str(self._db_filepath))
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
                    BitsAllocated INTEGER,
                    NumberOfFrames INTEGER,
                    TransferSyntaxUID TEXT NOT NULL,
                    _file_path TEXT,
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

    def _update_db(self):
        """Update database."""
        all_attributes = (
            self._attributes[_QueryResourceType.STUDIES] +
            self._attributes[_QueryResourceType.SERIES] +
            self._attributes[_QueryResourceType.INSTANCES]
        )
        tags = [
            tag_for_keyword(attr)
            for attr in all_attributes
        ]

        def is_stop_tag(tag: BaseTag, VR: Optional[str], length: int) -> bool:
            return tag > max(tags)

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
            found_file_paths.add(rel_file_path)
            if file_path in indexed_file_paths:
                logger.debug(f'skip indexed file {file_path}')
                continue

            logger.debug(f'index file {file_path}')
            with open(file_path, 'rb') as fp:
                try:
                    ds = read_partial(
                        fp,
                        stop_when=is_stop_tag,
                        specific_tags=tags
                    )
                except (InvalidDicomError, AttributeError):
                    logger.debug(f'failed to read file "{file_path}"')
                    continue

            if not hasattr(ds, 'SOPClassUID'):
                # This is probably a DICOMDIR file or some other weird thing
                continue

            try:
                study_metadata = self._extract_study_metadata(ds)
                study_instance_uid = ds.StudyInstanceUID
                studies[study_instance_uid] = tuple(study_metadata)

                series_metadata = self._extract_series_metadata(ds)
                series_instance_uid = ds.SeriesInstanceUID
                series[series_instance_uid] = tuple(series_metadata)

                instance_metadata = self._extract_instance_metadata(
                    ds,
                    rel_file_path
                )
                sop_instance_uid = ds.SOPInstanceUID
                instances[sop_instance_uid] = tuple(instance_metadata)
            except AttributeError as error:
                logger.warn(f'failed to parse file "{file_path}": {error}')
                continue

            if not i % n:
                # Insert every nth iteration to avoid having to read all
                # files again in case the update operation gets interrupted
                self._insert_into_db(
                    studies.values(),
                    series.values(),
                    instances.values()
                )

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
        # TODO: consider converting date and time to ISO format
        value = getattr(dataset, keyword, None)
        if value is None or isinstance(value, int):
            return value
        else:
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
        file_path: Union[Path, str]
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
        str,
        str,
    ]:
        metadata = [
            self._get_data_element_value(dataset, attr)
            for attr in self._attributes[_QueryResourceType.INSTANCES]
        ]
        metadata.append(str(dataset.file_meta.TransferSyntaxUID))
        metadata.append(str(file_path))
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
                str,
                str,
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
                'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
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

        self._delete_instances_from_db(
            uids=[
                (
                    r['StudyInstanceUID'],
                    r['SeriesInstanceUID'],
                    r['SOPInstanceUID'],
                )
                for r in results
            ]
        )

    def _delete_instances_from_db(
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
            if not item[0].startswith('_') and item[0] != 'TransferSyntaxUID'
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
                    keyword = self.lookup_keyword(key)
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

    def _get_studies(self) -> List[str]:
        self._cursor.execute('SELECT StudyInstanceUID FROM studies')
        results = self._cursor.fetchall()
        return [r['StudyInstanceUID'] for r in results]

    def _get_series(
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

    def _get_instances(
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

    def _get_image_file_reader(self, file_path: Path) -> _ImageFileReader:
        """Get the reader for a given image file.

        Parameters
        ----------
        file_path: pathlib.Path
            Path to the DICOM file containing a data set of an image

        Returns
        -------
        dicomweb_client.file._ImageFileReader
            Reader object

        Note
        ----
        The instance of the class caches reader object to improve performance
        for repeated frame-level file access.

        """
        try:
            image_file_reader = self._reader_cache[file_path]
            # Move the most recently retrieved entry to the beginning.
            self._reader_cache.move_to_end(file_path, last=False)
        except KeyError:
            image_file_reader = _ImageFileReader(file_path)
            image_file_reader.open()
            self._reader_cache[file_path] = image_file_reader
            if len(self._reader_cache) > self._max_reader_cache_size:
                # Remove the last entry.
                tmp_path, tmp_reader = self._reader_cache.popitem(last=False)
                tmp_reader.close()
        return image_file_reader

    def _get_instance_file_path(
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
        searchable_keywords.extend(
            self._attributes[_QueryResourceType.INSTANCES]
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
                'TransferSyntaxUID',
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
                    setattr(dataset, key, row[key])

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
        iterator = self.iter_bulkdata(
            url=url,
            media_types=media_types,
            byte_range=byte_range
        )
        return list(iterator)

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
        raise IOError(f'Resource does not exist: "{url}".')

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
        logger.info(
            'retrieve metadata of all instances '
            f'of study "{study_instance_uid}"'
        )
        series_index = self._get_series(study_instance_uid)
        collection = []
        for series_instance_uid, study_instance_uid in series_index:
            collection.extend(
                self.retrieve_series_metadata(
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                )
            )
        return collection

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
        series_index = self._get_series(study_instance_uid)
        for study_instance_uid, series_instance_uid in series_index:
            uids = self._get_instances(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
            )
            for study_instance_uid, series_instance_uid, sop_instance_uid in uids:  # noqa
                yield self.retrieve_instance(
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                    sop_instance_uid=sop_instance_uid,
                    media_types=media_types
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
        iterator = self.iter_study(
            study_instance_uid=study_instance_uid,
            media_types=media_types,
        )
        return list(iterator)

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
        instance_index = self._get_instances(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
        )
        for i in instance_index:
            study_instance_uid, series_instance_uid, sop_instance_uid = i
            yield self.retrieve_instance(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                sop_instance_uid=sop_instance_uid,
                media_types=media_types
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
        iterator = self.iter_series(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            media_types=media_types,
        )
        return list(iterator)

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
        raise ValueError('Retrieval of rendered series is not supported.')

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

        collection = []
        instance_index = self._get_instances(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
        )
        for i in instance_index:
            study_instance_uid, series_instance_uid, sop_instance_uid = i
            metadata = self.retrieve_instance_metadata(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                sop_instance_uid=sop_instance_uid,
            )
            collection.append(metadata)
        return collection

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
        file_path = self._get_instance_file_path(
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
        )
        metadata = dcmread(file_path, stop_before_pixels=True)
        return metadata.to_json_dict()

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

        file_path = self._get_instance_file_path(
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
            # media types, check whether the corresponding transfer syntax is
            # appropriate.
            expected_transfer_syntaxes = acceptable_media_type_lut[
                expected_media_type
            ]
            if (
                transfer_syntax_uid not in expected_transfer_syntaxes and
                '*' not in expected_transfer_syntaxes
            ):
                raise ValueError(
                    'Instance cannot be retrieved using media type "{}" '
                    'with any of the specified transfer syntaxes: "{}".'.format(
                        expected_media_type,
                        '", "'.join(expected_transfer_syntaxes)
                    )
                )

        if not found_matching_media_type:
            raise ValueError(
                'Instance cannot be retrieved using any of the '
                f'acceptable media types: {media_types}.'
            )

        return dataset

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
        file_path = self._get_instance_file_path(
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
        )
        image_file_reader = self._get_image_file_reader(file_path)
        metadata = image_file_reader.metadata
        if int(getattr(metadata, 'NumberOfFrames', '1')) > 1:
            raise ValueError(
                'Rendering of multi-frame image instance is not supported.'
            )
        frame_index = 0
        frame = image_file_reader.read_frame(frame_index)
        transfer_syntax_uid = image_file_reader.transfer_syntax_uid

        codec_name, codec_kwargs = self._get_image_codec_parameters(
            metadata=metadata,
            transfer_syntax_uid=transfer_syntax_uid,
            media_types=media_types,
            params=params
        )

        if codec_name is None:
            pixels = frame
        else:
            array = image_file_reader.decode_frame(frame_index, frame)
            image = Image.fromarray(array)
            with io.BytesIO() as fp:
                image.save(fp, codec_name, **codec_kwargs)  # type: ignore
                fp.seek(0)
                pixels = fp.read()

        return pixels

    def _check_media_types_for_instance_frames(
        self,
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
        if transfer_syntax_uid.is_encapsulated:
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
                raise ValueError(
                    'Instance frames cannot be retrieved using media type "{}" '
                    'with any of the specified transfer syntaxes: "{}".'.format(
                        expected_media_type,
                        '", "'.join(expected_transfer_syntaxes)
                    )
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
                raise ValueError(
                    'Instance frames cannot be retrieved using any of the '
                    f'acceptable media types: {media_types}.'
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
        file_path = self._get_instance_file_path(
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
        )

        if len(frame_numbers) == 0:
            raise ValueError('At least one frame number must be provided.')

        image_file_reader = self._get_image_file_reader(file_path)
        metadata = image_file_reader.metadata
        transfer_syntax_uid = image_file_reader.transfer_syntax_uid

        reencoding_media_type = self._check_media_types_for_instance_frames(
            transfer_syntax_uid,
            media_types
        )

        for frame_number in frame_numbers:
            frame_index = frame_number - 1
            frame = image_file_reader.read_frame(frame_index)

            if frame_number > int(getattr(metadata, 'NumberOfFrames', '1')):
                raise ValueError(
                    f'Provided frame number {frame_number} exceeds number '
                    'of available frames.'
                )

            if not transfer_syntax_uid.is_encapsulated:
                pixels = frame
            else:
                if reencoding_media_type is None:
                    pixels = frame
                elif reencoding_media_type == 'image/jp2':
                    image_type = 'jpeg2000'
                    image_kwargs = {'irreversible': False}
                    array = image_file_reader.decode_frame(frame_index, frame)
                    image = Image.fromarray(array)
                    with io.BytesIO() as fp:
                        image.save(
                            fp,
                            image_type,
                            **image_kwargs   # type: ignore
                        )
                        pixels = fp.getvalue()
                else:
                    raise ValueError(
                        'Cannot re-encode frames using media type '
                        f'"{reencoding_media_type}".'
                    )

            yield pixels

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
        file_path = self._get_instance_file_path(
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
        )

        if len(frame_numbers) == 0:
            raise ValueError('At least one frame number must be provided.')

        image_file_reader = self._get_image_file_reader(file_path)
        metadata = image_file_reader.metadata
        transfer_syntax_uid = image_file_reader.transfer_syntax_uid

        reencoding_media_type = self._check_media_types_for_instance_frames(
            transfer_syntax_uid,
            media_types
        )

        frame_indices = []
        for frame_number in frame_numbers:
            if frame_number > int(getattr(metadata, 'NumberOfFrames', '1')):
                raise ValueError(
                    f'Provided frame number {frame_number} exceeds number '
                    'of available frames.'
                )
            frame_index = frame_number - 1
            frame_indices.append(frame_index)

        frames = image_file_reader.read_frames(frame_indices, parallel=True)

        reencoded_frames = []
        for frame in frames:
            if not transfer_syntax_uid.is_encapsulated:
                reencoded_frame = frame
            else:
                if reencoding_media_type is None:
                    reencoded_frame = frame
                elif reencoding_media_type == 'image/jp2':
                    image_type = 'jpeg2000'
                    image_kwargs = {'irreversible': False}
                    array = image_file_reader.decode_frame(frame_index, frame)
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
                        'Cannot re-encode frames using media type '
                        f'"{reencoding_media_type}".'
                    )

            reencoded_frames.append(reencoded_frame)

        return reencoded_frames

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

        file_path = self._get_instance_file_path(
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
        )
        image_file_reader = self._get_image_file_reader(file_path)
        frame_index = frame_number - 1
        frame = image_file_reader.read_frame(frame_index)
        metadata = image_file_reader.metadata
        transfer_syntax_uid = image_file_reader.transfer_syntax_uid

        if frame_number > int(getattr(metadata, 'NumberOfFrames', '1')):
            raise ValueError(
                'Provided frame number exceeds number of frames.'
            )

        codec_name, codec_kwargs = self._get_image_codec_parameters(
            metadata=metadata,
            transfer_syntax_uid=transfer_syntax_uid,
            media_types=media_types,
            params=params
        )

        if codec_name is None:
            pixels = frame
        else:
            array = image_file_reader.decode_frame(frame_index, frame)
            image = Image.fromarray(array)
            with io.BytesIO() as fp:
                image.save(fp, codec_name, **codec_kwargs)
                fp.seek(0)
                pixels = fp.read()

        return pixels

    def _get_image_codec_parameters(
        self,
        metadata: Dataset,
        transfer_syntax_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]], ...]] = None,
        params: Optional[Dict[str, str]] = None,
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
                    logger.warn(
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
                icc_profile = metadata.OpticalPathSequence[0].ICCProfile
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
                str,
                str,
            ]
        ] = {}
        successes = []
        failures = []
        for ds in datasets:
            logger.info(
                f'store instance "{ds.SOPInstanceUID}" '
                f'of series "{ds.SeriesInstanceUID}" '
                f'of study "{ds.StudyInstanceUID}" '
            )

            try:
                if study_instance_uid is not None:
                    if ds.StudyInstanceUID != study_instance_uid:
                        continue
                else:
                    study_instance_uid = ds.StudyInstanceUID
                study_metadata = self._extract_study_metadata(ds)
                studies[study_instance_uid] = study_metadata

                series_metadata = self._extract_series_metadata(ds)
                series_instance_uid = ds.SeriesInstanceUID
                series[series_instance_uid] = series_metadata

                sop_instance_uid = ds.SOPInstanceUID
                rel_file_path = '/'.join([
                    'studies',
                    study_instance_uid,
                    'series',
                    series_instance_uid,
                    'instances',
                    sop_instance_uid
                ])
                instance_metadata = self._extract_instance_metadata(
                    ds,
                    rel_file_path
                )
                instances[sop_instance_uid] = instance_metadata

                with io.BytesIO() as b:
                    dcmwrite(b, ds, write_like_original=False)
                    file_content = b.getvalue()

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

        response = Dataset()
        response.RetrieveURL = None

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
                success_item.RetrieveURL = None

        if len(failures) > 0:
            response.FailedSOPSequence = []
            for ds in failures:
                failure_item = Dataset()
                failure_item.FailureReason = 272
                failure_item.ReferencedSOPClassUID = ds.SOPClassUID
                failure_item.ReferencedSOPInstanceUID = ds.SOPInstanceUID
                response.FailedSOPSequence.append(failure_item)

        return response

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
        uids = self._get_instances(study_instance_uid)
        for study_instance_uid, series_instance_uid, sop_instance_uid in uids:
            self.delete_instance(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                sop_instance_uid=sop_instance_uid,
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
        uids = self._get_instances(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
        )
        for study_instance_uid, series_instance_uid, sop_instance_uid in uids:
            self.delete_instance(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                sop_instance_uid=sop_instance_uid,
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
        file_path = self._get_instance_file_path(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid,
        )
        self._delete_instances_from_db(
            uids=[
                (study_instance_uid, series_instance_uid, sop_instance_uid)
            ]
        )
        os.remove(file_path)
