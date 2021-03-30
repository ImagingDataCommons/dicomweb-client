"""Utilities for DICOMweb URI manipulation."""
import enum
import re
from typing import Optional, Sequence, Tuple
import urllib.parse as urlparse


class URIType(enum.Enum):
    """Type of DICOM resource the URI points to."""
    SERVICE = 'service'
    STUDY = 'study'
    SERIES = 'series'
    INSTANCE = 'instance'
    FRAME = 'frame'


class URISuffix(enum.Enum):
    """Optional suffixes for a DICOM resource."""
    METADATA = 'metadata'
    RENDERED = 'rendered'
    THUMBNAIL = 'thumbnail'


# For DICOM Standard spec validation of UID components in `URI`.
_MAX_UID_LENGTH = 64
_REGEX_UID = re.compile(r'[0-9]+([.][0-9]+)*')
_REGEX_PERMISSIVE_UID = re.compile(r'[^/@]+')


class URI:
    """Class to represent a fully qualified HTTP[S] URI to a DICOMweb resource.

    http://dicom.nema.org/medical/dicom/current/output/html/part18.html

    This is an immutable class. Use `URI.update()` to create copies of an
    instance with updated (new) values for its attributes.

    Given an HTTP[S] `base_url`, a valid DICOMweb-compatible URI would be:

    - ``<base_url>`` (no DICOMweb suffix)
    - ``<base_url>/studies/<study_instance_uid>``
    - ``<base_url>/studies/<study_instance_uid>/metadata``
    - ``<base_url>/studies/<study_instance_uid>/rendered``
    - ``<base_url>/studies/<study_instance_uid>/thumbnail``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/metadata``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/rendered``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/thumbnail``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/instances/<sop_instance_uid>``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/instances/<sop_instance_uid>/metadata``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/instances/<sop_instance_uid>/rendered``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/instances/<sop_instance_uid>/thumbnail``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/instances/<sop_instance_uid>/frames/<frames>``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/instances/<sop_instance_uid>/frames/<frames>/rendered``
    - ``<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/instances/<sop_instance_uid>/frames/<frames>/thumbnail``
    """  # noqa

    def __init__(self,
                 base_url: str,
                 study_instance_uid: Optional[str] = None,
                 series_instance_uid: Optional[str] = None,
                 sop_instance_uid: Optional[str] = None,
                 frames: Optional[Sequence[int]] = None,
                 suffix: Optional[URISuffix] = None,
                 permissive: bool = False):
        """Instantiates an object.

        As per the DICOM Standard, the Study, Series, and Instance UIDs must be
        a series of numeric components (``0``-``9``) separated by the period
        ``.`` character, with a maximum length of 64 characters.
        If the ``permissive`` flag is set to ``True``, any alpha-numeric or
        special characters (except for ``/`` and ``@``) may be used.

        Parameters
        ----------
        base_url: str
            DICOMweb service HTTP[S] URL. Trailing forward slashes are not
            permitted.
        study_instance_uid: str, optional
            DICOM Study Instance UID.
        series_instance_uid: str, optional
            DICOM Series Instance UID.
        sop_instance_uid: str, optional
            DICOM SOP Instance UID.
        frames: Sequence[int], optional
            A non-empty sequence of positive frame numbers in ascending order.
        suffix: URISuffix, optional
            Suffix attached to the DICOM resource URI. This could refer to a
            metadata, rendered, or thumbnail resource.
        permissive: bool
            If ``True``, relaxes the DICOM Standard validation for UIDs (see
            main docstring for details). This option is made available since
            users may be occasionally forced to work with DICOMs or services
            that may be in violation of the standard. Unless required, use of
            this flag is **not** recommended, since non-conformant UIDs may
            lead to unexpected errors downstream, e.g., rejection by a DICOMweb
            server, etc.

        Raises
        ------
        ValueError
            In the following cases:

            - `base_url` has a trailing slash.
            - `base_url` does not use the HTTP[S] addressing scheme.
            - `base_url` is incompatible with the DICOMweb standard.
            - `series_instance_uid` is supplied without `study_instance_uid`.
            - `sop_instance_uid` is supplied without `study_instance_uid` or
              `series_instance_uid`.
            - `frames` is supplied without `study_instance_uid`,
              `series_instance_uid`, or `sop_instance_uid`.
            - `frames` is empty.
            - A frame number in `frames` is not positive.
            - The frame numbers in `frames` are not in ascending order.
            - `suffix` is :py:attr:`URISuffix.METADATA` with `frames` or
              without `study_instance_uid`.
            - `suffix` is :py:attr:`URISuffix.RENDERED` without
              `study_instance_uid`.
            - `suffix` is :py:attr:`URISuffix.THUMBNAIL` without
              `study_instance_uid`.
            - Any one of `study_instance_uid`, `series_instance_uid`, or
              `sop_instance_uid` does not meet the DICOM Standard UID spec in
              the docstring.
        """
        _validate_base_url(base_url)
        _validate_resource_identifiers_and_suffix(
            permissive,
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid,
            frames,
            suffix,
        )
        self._base_url = base_url
        self._study_instance_uid = study_instance_uid
        self._series_instance_uid = series_instance_uid
        self._instance_uid = sop_instance_uid
        self._frames = None if frames is None else tuple(frames)
        self._suffix = suffix
        self._permissive = permissive

    def __str__(self) -> str:
        """Returns the object as a DICOMweb URI string."""
        frames = None if not self.frames else ','.join(
            str(frame_number) for frame_number in self.frames
        )
        parts = (
            ('studies', self.study_instance_uid),
            ('series', self.series_instance_uid),
            ('instances', self.sop_instance_uid),
            ('frames', frames),
        )
        dicomweb_suffix = '/'.join(f'{part}/{part_value}'
                                   for part, part_value in parts
                                   if part_value is not None)
        if self.suffix is not None:
            dicomweb_suffix = f'{dicomweb_suffix}/{self.suffix.value}'
        # Remove the trailing slash in case `dicomweb_suffix` is empty.
        return f'{self.base_url}/{dicomweb_suffix}'.rstrip('/')

    def __hash__(self) -> int:
        """Returns a hash for the object."""
        return hash((self.base_url, self.study_instance_uid,
                     self.series_instance_uid, self.sop_instance_uid,
                     self.frames, self.suffix))

    def __repr__(self) -> str:
        """Returns an "official" string representation of this object."""
        return (f'dicomweb_client.URI(base_url={self.base_url!r}, '
                f'study_instance_uid={self.study_instance_uid!r}, '
                f'series_instance_uid={self.series_instance_uid!r}, '
                f'sop_instance_uid={self.sop_instance_uid!r}, '
                f'frames={self.frames!r}, suffix={self.suffix!r})')

    def __eq__(self, other: object) -> bool:
        """Compares the object for equality with `other`."""
        if not isinstance(other, URI):
            return NotImplemented
        return str(self) == str(other)

    @property
    def base_url(self) -> str:
        """Returns the Base (DICOMweb Service) URL."""
        return self._base_url

    @property
    def study_instance_uid(self) -> Optional[str]:
        """Returns the Study UID, if available."""
        return self._study_instance_uid

    @property
    def series_instance_uid(self) -> Optional[str]:
        """Returns the Series UID, if available."""
        return self._series_instance_uid

    @property
    def sop_instance_uid(self) -> Optional[str]:
        """Returns the Instance UID, if available."""
        return self._instance_uid

    @property
    def frames(self) -> Optional[Tuple[int, ...]]:
        """Returns the sequence of frame numbers, if available."""
        return self._frames

    @property
    def suffix(self) -> Optional[URISuffix]:
        """Returns the DICOM resource suffix, if available."""
        return self._suffix

    @property
    def type(self) -> URIType:
        """The `URIType` of DICOM resource referenced by the object."""
        if self.study_instance_uid is None:
            return URIType.SERVICE
        elif self.series_instance_uid is None:
            return URIType.STUDY
        elif self.sop_instance_uid is None:
            return URIType.SERIES
        elif self.frames is None:
            return URIType.INSTANCE
        return URIType.FRAME

    @property
    def permissive(self) -> bool:
        """Returns the ``permissive`` parameter value in the initializer."""
        return self._permissive

    def base_uri(self) -> 'URI':
        """Returns `URI` for the DICOM Service within this object."""
        return URI(self.base_url)

    def study_uri(self) -> 'URI':
        """Returns `URI` for the DICOM Study within this object."""
        if self.type == URIType.SERVICE:
            raise ValueError('Cannot get a Study URI from a Base (DICOMweb '
                             'service) URL.')
        return URI(self.base_url, self.study_instance_uid)

    def series_uri(self) -> 'URI':
        """Returns `URI` for the DICOM Series within this object."""
        if self.type in (URIType.SERVICE, URIType.STUDY):
            raise ValueError(
                f'Cannot get a Series URI from a {self.type!r} URI.')
        return URI(self.base_url, self.study_instance_uid,
                   self.series_instance_uid)

    def instance_uri(self) -> 'URI':
        """Returns `URI` for the DICOM Instances within this object."""
        if self.type not in (URIType.INSTANCE, URIType.FRAME):
            raise ValueError(
                f'Cannot get an Instance URI from a {self.type!r} URI.')
        return URI(self.base_url, self.study_instance_uid,
                   self.series_instance_uid, self.sop_instance_uid)

    def frame_uri(self) -> 'URI':
        """Returns `URI` for the DICOM frames within this object."""
        if self.type != URIType.FRAME:
            raise ValueError(
                f'Cannot get a Frame URI from a {self.type!r} URI.')
        return URI(self.base_url, self.study_instance_uid,
                   self.series_instance_uid, self.sop_instance_uid, self.frames)

    def update(self,
               base_url: Optional[str] = None,
               study_instance_uid: Optional[str] = None,
               series_instance_uid: Optional[str] = None,
               sop_instance_uid: Optional[str] = None,
               frames: Optional[Sequence[int]] = None,
               suffix: Optional[URISuffix] = None,
               permissive: Optional[bool] = False) -> 'URI':
        """Creates a new `URI` object based on the current one.

        Replaces the specified `URI` components in the current `URI` to create
        the new one.

        Parameters
        ----------
        base_url: str, optional
            DICOMweb service HTTP[S] URL to use in the new `URI` or `None` if
            the `base_url` from the current `URI` should be used.
        study_instance_uid: str, optional
            Study Instance UID to use in the new `URI` or `None` if the
            `study_instance_uid` from the current `URI` should be used.
        series_instance_uid: str, optional
            Series Instance UID to use in the new `URI` or `None` if the
            `series_instance_uid` from the current `URI` should be used.
        sop_instance_uid: str, optional
            SOP Instance UID to use in the new `URI` or `None` if the
            `sop_instance_uid` from the current `URI` should be used.
        frames: Sequence[int], optional
            Frame numbers to use in the new `URI` or `None` if the `frames`
            from the current `URI` should be used.
        suffix: URISuffix, optional
            Suffix to use in the new `URI` or `None` if the `suffix` from the
            current `URI` should be used.
        permissive: bool, optional
            Set if permissive handling of UIDs (if any) in the updated ``URI``
            is required. See the class initializer docstring for details.

        Returns
        -------
        URI
          The newly constructed `URI` object.

        Raises
        ------
        ValueError
            If the new `URI` is invalid (e.g., if only the SOP Instance UID is
            specified, but the Series Instance UID is missing in the current
            `URI`).
        """
        return URI(
            base_url if base_url is not None else self.base_url,
            study_instance_uid
            if study_instance_uid is not None else self.study_instance_uid,
            series_instance_uid
            if series_instance_uid is not None else self.series_instance_uid,
            sop_instance_uid
            if sop_instance_uid is not None else self.sop_instance_uid,
            frames if frames is not None else self.frames,
            suffix if suffix is not None else self.suffix,
            permissive if permissive is not None else self.permissive,
        )

    @property
    def parent(self) -> 'URI':
        """Returns a URI to the "parent" resource.

        Depending on the `type` of the current `URI`, the `URI` of the parent
        resource is defined as:

        +----------+----------+----------+
        | Current  | Suffixed | Parent   |
        +==========+==========+==========+
        | Service  | N/A      | Service  |
        +----------+----------+----------+
        | Study    | No       | Service  |
        +----------+----------+----------+
        | Study    | Yes      | Study    |
        +----------+----------+----------+
        | Series   | No       | Study    |
        +----------+----------+----------+
        | Series   | Yes      | Series   |
        +----------+----------+----------+
        | Instance | No       | Series   |
        +----------+----------+----------+
        | Instance | Yes      | Instance |
        +----------+----------+----------+
        | Frame    | No       | Instance |
        +----------+----------+----------+
        | Frame    | Yes      | Frame    |
        +----------+----------+----------+

        Returns
        -------
        URI
          An instance pointing to the parent resource.
        """
        if self.type == URIType.SERVICE or self.suffix is not None:
            return URI(
                self.base_url,
                self.study_instance_uid,
                self.series_instance_uid,
                self.sop_instance_uid,
                self.frames,
                suffix=None)
        elif self.type == URIType.STUDY:
            return URI(self.base_url)
        elif self.type == URIType.SERIES:
            return self.study_uri()
        elif self.type == URIType.INSTANCE:
            return self.series_uri()
        else:
            return self.instance_uri()

    @classmethod
    def from_string(cls,
                    dicomweb_uri: str,
                    uri_type: Optional[URIType] = None,
                    permissive: bool = False) -> 'URI':
        """Parses the string to return the URI.

        Any valid DICOMweb compatible HTTP[S] URI is permitted, e.g.,
        ``<SERVICE>/studies/<StudyInstanceUID>/series/<SeriesInstanceUID>``.

        Parameters
        ----------
        dicomweb_uri: str
            An HTTP[S] DICOMweb-compatible URI.
        uri_type: URIType, optional
            The expected DICOM resource type referenced by the object. If set,
            it validates that the resource-scope of the `dicomweb_uri` matches
            the expected type.
        permissive: bool
            Set if permissive handling of UIDs (if any) in ``dicomweb_uri`` is
            required. See the class initializer docstring for details.

        Returns
        -------
        URI
            The newly constructed `URI` object.

        Raises
        ------
        ValueError
            If the URI cannot be parsed or the actual URI type doesn't match
            the specified expected `uri_type`.
        """
        (study_instance_uid,
         series_instance_uid,
         sop_instance_uid,
         frames,
         suffix) = (None, None, None, None, None)
        # The URI format validation will happen when `URI` is returned at the
        # end.
        base_url_and_resource_suffix = dicomweb_uri.rsplit(
            '/studies/', maxsplit=1)
        base_url = base_url_and_resource_suffix[0]

        if len(base_url_and_resource_suffix) > 1:
            resource_suffix = f'studies/{base_url_and_resource_suffix[1]}'
            parts = resource_suffix.split('/')
            while parts:
                part = parts.pop(0)
                if part == 'studies' and parts:
                    study_instance_uid = parts.pop(0)
                elif (part == 'series' and
                      study_instance_uid is not None and parts):
                    series_instance_uid = parts.pop(0)
                elif (part == 'instances' and
                      series_instance_uid is not None and parts):
                    sop_instance_uid = parts.pop(0)
                elif (part == 'frames' and
                      sop_instance_uid is not None and parts):
                    frames_csv = parts.pop(0)
                    try:
                        frames = tuple(int(frame_number) for frame_number in
                                       frames_csv.split(','))
                    except ValueError as e:
                        raise ValueError('Found non-integral frame numbers in '
                                         f'frame list: {frames!r}') from e
                elif part == URISuffix.METADATA.value and not parts:
                    # This check sticks out. Consider a cleaner codepath?
                    if frames is not None:
                        raise ValueError(
                            f'DICOMweb URI {dicomweb_uri!r} for frames '
                            'resource cannot have "metadata" suffix.')
                    suffix = URISuffix.METADATA
                elif part in (URISuffix.RENDERED.value,
                              URISuffix.THUMBNAIL.value) and not parts:
                    suffix = URISuffix(part)
                else:
                    raise ValueError(
                        f'Error parsing the suffix {resource_suffix!r} from '
                        f'URI: {dicomweb_uri!r}')

        uri = cls(base_url, study_instance_uid, series_instance_uid,
                  sop_instance_uid, frames, suffix, permissive)
        # Validate that the URI is of the specified type, if applicable.
        if uri_type is not None and uri.type != uri_type:
            raise ValueError(
                f'Unexpected URI type. Expected: {uri_type!r}, Actual: '
                f'{uri.type!r}. URI: {dicomweb_uri!r}')

        return uri


def _validate_base_url(url: str) -> None:
    """Validates the Base (DICOMweb service) URL supplied to `URI`."""
    parse_result = urlparse.urlparse(url)
    if parse_result.scheme not in ('http', 'https'):
        raise ValueError(
            f'Only HTTP[S] URLs are permitted. Actual URL: {url!r}')
    if url.endswith('/'):
        raise ValueError('Base (DICOMweb service) URL cannot have a trailing '
                         f'forward slash: {url!r}')


def _validate_resource_identifiers_and_suffix(
        permissive: bool,
        study_instance_uid: Optional[str],
        series_instance_uid: Optional[str],
        sop_instance_uid: Optional[str],
        frames: Optional[Sequence[int]],
        suffix: Optional[URISuffix]) -> None:
    """Validates UID, frames, and suffix params for the `URI` constructor."""
    # Note that the order of comparisons in this method is important.
    if series_instance_uid is not None and study_instance_uid is None:
        raise ValueError('`study_instance_uid` missing with non-empty '
                         f'`series_instance_uid`: {series_instance_uid!r}')

    if sop_instance_uid is not None and series_instance_uid is None:
        raise ValueError('`series_instance_uid` missing with non-empty '
                         f'`sop_instance_uid`: {sop_instance_uid!r}')

    if frames is not None:
        if sop_instance_uid is None:
            raise ValueError('`sop_instance_uid` missing with non-empty '
                             f'`frames`: {frames}')
        _validate_frames(frames)

    for uid in (study_instance_uid, series_instance_uid, sop_instance_uid):
        if uid is not None:
            _validate_uid(uid, permissive)

    if suffix in (URISuffix.RENDERED, URISuffix.THUMBNAIL) and (
            study_instance_uid is None):
        raise ValueError(
            f'{suffix!r} suffix requires a DICOM resource pointer, and cannot '
            'be set for a Service URL alone.')

    if suffix == URISuffix.METADATA and (study_instance_uid is None or
                                         frames is not None):
        raise ValueError(f'{suffix!r} suffix may only be set for the DICOM '
                         'resources: Study, Series, or SOP Instance UID')


def _validate_uid(uid: str, permissive: bool) -> None:
    """Validates a DICOM UID."""
    if len(uid) > _MAX_UID_LENGTH:
        raise ValueError('UID cannot have more than 64 chars. '
                         f'Actual count in {uid!r}: {len(uid)}')
    if not permissive and _REGEX_UID.fullmatch(uid) is None:
        raise ValueError(f'UID {uid!r} must match regex {_REGEX_UID!r} in '
                         'conformance with the DICOM Standard.')
    elif permissive and _REGEX_PERMISSIVE_UID.fullmatch(uid) is None:
        raise ValueError(f'Permissive mode is enabled. UID {uid!r} must match '
                         f'regex {_REGEX_PERMISSIVE_UID!r}.')


def _validate_frames(frames: Sequence[int]) -> None:
    """Validates frame numbers to ensure non-empty list with positive values."""
    if not frames:
        raise ValueError('`frames` cannot be empty.')

    non_positive_frame_numbers = tuple(
        frame_number for frame_number in frames if frame_number < 1)
    if non_positive_frame_numbers:
        raise ValueError('Frame numbers must be positive. Found violations: '
                         f'{non_positive_frame_numbers!r}')

    # Python uses Timsort which is `O(n)` in the best case, i.e., the overhead
    # is negligible assuming most inputs meet this specification. If the
    # specification is violated, `n` is small in case of DICOMs (few hundreds
    # in the worst case?). Here, the simplicity of the implementation outweighs
    # the (roughly constant time) overhead.
    if tuple(sorted(frames)) != tuple(frames):
        raise ValueError('Frame numbers must be in ascending order. Actual '
                         f'order: {frames!r}')
