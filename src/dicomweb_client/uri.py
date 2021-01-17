"""Utilities for DICOMweb path manipulation."""
import enum
import re
from typing import Optional, Tuple
import urllib.parse as urlparse


class URIType(enum.Enum):
    """Type of DICOM resource the path points to."""
    SERVICE = 'service'
    STUDY = 'study'
    SERIES = 'series'
    INSTANCE = 'instance'


# For DICOM Standard spec validation of path UID components.
_MAX_UID_LENGTH = 64
_REGEX_UID = re.compile(r'[0-9]+([.][0-9]+)*')


class URI:
    """Class to represent a fully qualified HTTPS URI to a DICOMweb resource.

    http://dicom.nema.org/dicom/2013/output/chtml/part18/sect_6.7.html

    Given an HTTPS *base_url*, a valid DICOMweb-compatible URI would be:
    - '<base_url>' (no DICOMWeb suffix)
    - '<base_url>/studies/<study_instance_uid>'
    - '<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>'
    - '<base_url>/studies/<study_instance_uid>/series/<series_instance_uid>/ \
        instances/<sop_instance_uid>'
    """
    def __init__(self,
                 base_url: str,
                 study_instance_uid: Optional[str] = None,
                 series_instance_uid: Optional[str] = None,
                 sop_instance_uid: Optional[str] = None):
        """Instantiates an object.

        As per the DICOM Standard, the Study, Series, and Instance UIDs must be
        a series of numeric components ("0"-"9") separated by the period "."
        character, with a maximum length of 64 characters.

        Parameters
        ----------
        base_url: str
            DICOMweb service HTTPS URI. Trailing forward slashes are not
            permitted.
        study_instance_uid: str, optional
            DICOM Study UID.
        series_instance_uid: str, optional
            DICOM Series UID.
        sop_instance_uid: str, optional
            DICOM SOP Instance UID.

        Raises
        ------
        ValueError:
            In the following cases:
            - *base_url* has a trailing slash.
            - *base_url* does not use the HTTPS addressing scheme.
            - *base_url* is incompatible with the DICOMweb standard.
            - *series_instance_uid* is supplied without *study_instance_uid*.
            - *sop_instance_uid* is supplied without *study_instance_uid* or
              *series_instance_uid*.
            - Any one of *study_instance_uid*, *series_instance_uid*, or
              *sop_instance_uid* does not meet the DICOM Standard UID spec in
              the docstring.
        """
        _validate_base_url(base_url)
        _validate_uids(study_instance_uid, series_instance_uid,
                       sop_instance_uid)
        self._base_url = base_url
        self._study_instance_uid = study_instance_uid
        self._series_instance_uid = series_instance_uid
        self._instance_uid = sop_instance_uid

    def __str__(self) -> str:
        """Returns the path as a DICOMweb URI string."""
        parts = (('studies', self.study_instance_uid),
                 ('series', self.series_instance_uid), ('instances',
                                                        self.sop_instance_uid))
        dicomweb_suffix = '/'.join(f'{part}/{part_uid}'
                                   for part, part_uid in parts
                                   if part_uid is not None)
        # Remove the trailing slash in case the suffix is empty.
        return f'{self.base_url}/{dicomweb_suffix}'.rstrip('/')

    def __hash__(self) -> int:
        """Returns a hash for the object."""
        return hash((self.base_url, self.study_instance_uid,
                     self.series_instance_uid, self.sop_instance_uid))

    def __repr__(self) -> str:
        """Returns an "official" string representation of this object."""
        return (f'dicomweb_client.URI(base_url={self.base_url!r}, '
                f'study_instance_uid={self.study_instance_uid!r}, '
                f'series_instance_uid={self.series_instance_uid!r}, '
                f'sop_instance_uid={self.sop_instance_uid!r})')

    def __eq__(self, other: 'URI') -> bool:
        """Compares the object for equality with *other*."""
        return str(self) == str(other)

    @property
    def base_url(self) -> str:
        """Returns the Base (DICOMweb service) URI."""
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
    def type(self) -> URIType:
        """The *URIType* of DICOM resource referenced by the path."""
        if self.study_instance_uid is None:
            return URIType.SERVICE
        elif self.series_instance_uid is None:
            return URIType.STUDY
        elif self.sop_instance_uid is None:
            return URIType.SERIES
        return URIType.INSTANCE

    def study_subpath(self) -> 'URI':
        """Returns the sub-path for the DICOM Study within this path."""
        if self.type == URIType.SERVICE:
            raise ValueError('Cannot get a Study path from a Base (DICOMweb '
                             'service) path.')
        return URI(self.base_url, self.study_instance_uid)

    def series_subpath(self) -> 'URI':
        """Returns the sub-path for the DICOM Series within this path."""
        if self.type in (URIType.SERVICE, URIType.STUDY):
            raise ValueError(
                f'Cannot get a Series path from a {self.type!r} path.')
        return URI(self.base_url, self.study_instance_uid,
                   self.series_instance_uid)

    def update(self,
               base_url: Optional[str] = None,
               study_instance_uid: Optional[str] = None,
               series_instance_uid: Optional[str] = None,
               sop_instance_uid: Optional[str] = None) -> 'URI':
        """Creates a new `URI` object based on the current one.

        Replaces the specified `URI` components in the current `URI` to create
        the new one.

        Parameters
        ----------
        base_url: str, optional
            DICOMweb service HTTPS URL to use in the new `URI` or `None` if the
            *base_url* from the current `URI` should be used.
        study_instance_uid: str, optional
            Study Instance UID to use in the new `URI` or `None` if the
            *study_instance_uid* from the current `URI` should be used.
        series_instance_uid: str, optional
            Series Instance UID to use in the new `URI` or `None` if the
            *series_instance_uid* from the current `URI` should be used.
        sop_instance_uid: str, optional
            SOP Instance UID to use in the new `URI` or `None` if the
            *sop_instance_uid* from the current `URI` should be used.

        Returns
        -------
        The newly constructed `URI` object.

        Raises
        ------
        ValueError:
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
        )

    @property
    def parent(self) -> 'URI':
        """Returns a sub-path to the "parent" resource.

        Depending on the `type` of the current path, the sub-path of the parent
        resource is defined as:

        +--------------------+
        | Current  | Parent  |
        +--------------------+
        | Service  | Service |
        | Study    | Service |
        | Series   | Study   |
        | Instance | Series  |
        +--------------------+

        Returns
        -------
        An instance of the parent resource sub-path.
        """
        if self.type == URIType.SERVICE:
            return self
        elif self.type == URIType.STUDY:
            return self.base_url
        elif self.type == URIType.SERIES:
            return self.study_subpath()
        else:
            return self.series_subpath()

    @property
    def parts(self) -> Tuple[str]:
        """Returns the sequence of URI components in a *tuple*.

        For example, if the path is:
        https://service.com/studies/1.2.3/series/4.5.6

        then the method returns ['https://service.com', '1.2.3', '4.5.6']

        Returns
        -------
        Tuple[str]
            Sequence of URI components.
        """
        return tuple(part for part in (self.base_url, self.study_instance_uid,
                                       self.series_instance_uid,
                                       self.sop_instance_uid)
                     if part is not None)

    @classmethod
    def from_string(cls,
                    dicomweb_url: str,
                    path_type: Optional[URIType] = None) -> 'URI':
        """Parses the string to return the URI.

        Any valid DICOMweb compatible HTTPS URI is permitted, e.g.,
        '<SERVICE>/studies/<StudyInstanceUID>/series/<SeriesInstanceUID>'

        Parameters
        ----------
        dicomweb_url: str
            An HTTPS DICOMweb-compatible URI.
        path_type: URIType, optional
            The expected DICOM resource type referenced by the path. If set, it
            validates that the resource-scope of the *dicomweb_url* matches the
            expected type.

        Returns
        -------
        URI
            The newly constructed *URI* object.

        Raises
        ------
        ValueError:
            If the path cannot be parsed or the actual path type doesn't match
            the specified expected *path_type*.
        """
        study_instance_uid, series_instance_uid, sop_instance_uid = (None,
                                                                     None,
                                                                     None)
        # The URI format validation will happen when *URI* is returned at the
        # end.
        base_url_and_suffix = dicomweb_url.rsplit('/studies/', maxsplit=1)
        base_url = base_url_and_suffix[0]

        if len(base_url_and_suffix) > 1:
            dicomweb_suffix = f'studies/{base_url_and_suffix[1]}'
            parts = dicomweb_suffix.split('/')
            while parts:
                part = parts.pop(0)
                if part == 'studies' and parts:
                    study_instance_uid = parts.pop(0)
                elif (part == 'series' and study_instance_uid is not None
                      and parts):
                    series_instance_uid = parts.pop(0)
                elif (part == 'instances' and series_instance_uid is not None
                      and parts):
                    sop_instance_uid = parts.pop(0)
                else:
                    raise ValueError(
                        f'Error parsing the suffix {dicomweb_suffix!r} from '
                        f'URI: {dicomweb_url!r}')

        path = cls(base_url, study_instance_uid, series_instance_uid,
                   sop_instance_uid)
        # Validate that the path is of the specified type, if applicable.
        if path_type is not None and path.type != path_type:
            raise ValueError(
                f'Unexpected path type. Expected: {path_type!r}, Actual: '
                f'{path.type!r}. URI: {dicomweb_url!r}')

        return path


def _validate_base_url(url: str) -> None:
    """Validates the Base (DICOMweb service) URI supplied to `URI`."""
    parse_result = urlparse.urlparse(url)
    if parse_result.scheme != 'https':
        raise ValueError(f'Not an HTTPS URI: {url!r}')
    if url.endswith('/'):
        raise ValueError('Base (DICOMweb service) URI cannot have a trailing '
                         f'forward slash: {url!r}')


def _validate_uids(study_instance_uid: Optional[str],
                   series_instance_uid: Optional[str],
                   sop_instance_uid: Optional[str]) -> None:
    """Validates UID parameters for the `URI` constructor."""
    if study_instance_uid is None and not (series_instance_uid is None
                                           and sop_instance_uid is None):
        raise ValueError(
            'study_instance_uid missing with non-empty series_instance_uid or '
            f'sop_instance_uid. series_instance_uid: {series_instance_uid!r}, '
            f'sop_instance_uid: {sop_instance_uid!r}')

    if series_instance_uid is None and sop_instance_uid is not None:
        raise ValueError(
            'series_instance_uid missing with non-empty sop_instance_uid. '
            f'sop_instance_uid: {sop_instance_uid!r}')

    for uid in (study_instance_uid, series_instance_uid, sop_instance_uid):
        if uid is not None:
            _validate_uid(uid)


def _validate_uid(uid: str) -> None:
    """Validates a DICOM UID."""
    if len(uid) > _MAX_UID_LENGTH:
        raise ValueError('UID cannot have more than 64 chars. '
                         f'Actual count in {uid!r}: {len(uid)}')
    if _REGEX_UID.fullmatch(uid) is None:
        raise ValueError(f'UID {uid!r} must match regex {_REGEX_UID!r}.')
