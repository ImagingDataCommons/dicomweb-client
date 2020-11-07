"""Utilities for DICOMweb path manipulation."""
import attr
import enum
from typing import Optional
import urllib.parse as urlparse


class Type(enum.Enum):
    """Type of DICOM resource the path points to."""
    SERVICE = 'service'
    STUDY = 'study'
    SERIES = 'series'
    INSTANCE = 'instance'


# Used for DICOM UIDs validation
# '/' is not allowed because the parsing logic in the class uses '/' to tokenize
# the path.
# '@' is not allowed due to security concerns: theoretically it could lead to
# the part before '@' being interpreted as the username, and the part after -
# as the server address, which is a potential vulnerability.
_REGEX_UID = r'[^/@]+'
_ATTR_VALIDATOR_UID = attr.validators.optional(
    attr.validators.matches_re(_REGEX_UID))


@attr.s(frozen=True)
class Path(object):
    """Represents a fully qualified HTTPS URL to a DICOMweb resource.

    http://dicom.nema.org/dicom/2013/output/chtml/part18/sect_6.7.html

    Given an HTTPS *service_url*, a valid DICOMweb-compatible URL would be:
    - '<service_url>' (no DICOMWeb suffix)
    - '<service_url>/studies/<study_uid>'
    - '<service_url>/studies/<study_uid>/series/<series_uid>'
    - '<service_url>/studies/<study_uid>/series/<series_uid>/instances/ \
        <instance_uid>'

    Attributes
    ----------
    service_url: str
        DICOMweb service HTTPS URL. Trailing forward slashes are not permitted.
    study_uid: str
        DICOM Study UID. Alphanumeric characters with '.' separator are
        permitted.
    series_uid: str
        DICOM Series UID. Alphanumeric characters with '.' separator are
        permitted.
    instance_uid: str
        DICOM Instance UID. Alphanumeric character with '.' separator are
        permitted.

    Raises
    ------
    ValueError:
        In the following cases:
        - *service_url* has a trailing slash.
        - *service_url* does not use the HTTPS addressing scheme.
        - *service_url* is incompatible with the DICOMweb standard.
        - *series_uid* is supplied without *study_uid*.
        - *instance_uid* is supplied without *study_uid* or *series_uid*.
    """
    service_url = attr.ib(type=str)
    study_uid = attr.ib(
        default=None, type=Optional[str], validator=_ATTR_VALIDATOR_UID)
    series_uid = attr.ib(
        default=None, type=Optional[str], validator=_ATTR_VALIDATOR_UID)
    instance_uid = attr.ib(
        default=None, type=Optional[str], validator=_ATTR_VALIDATOR_UID)

    @service_url.validator
    def _not_https(self, _, value: str):
        parse_result = urlparse.urlparse(value)
        if parse_result.scheme != 'https':
            raise ValueError(f'Not an HTTPS url: {value}')

    @service_url.validator
    def _trailing_forward_slash(self, _, value: str):
        if value.endswith('/'):
            raise ValueError(
                f'Service URL cannot have a trailing forward slash: {value}')

    @study_uid.validator
    def _study_uid_missing(self, _, value: Optional[str]):
        if value is None and not (self.series_uid is None and
                                  self.instance_uid is None):
            raise ValueError(
                'study_uid missing with non-empty series_uid or instance_uid. '
                f'series_uid: {self.series_uid}, instance_uid: '
                f'{self.instance_uid}')

    @series_uid.validator
    def _series_uid_missing(self, _, value: Optional[str]) -> None:
        if value is None and self.instance_uid is not None:
            raise ValueError('series_uid missing with non-empty instance_uid. '
                             f'instance_uid: {self.instance_uid}')

    def __str__(self):
        """Returns the text representation of the path."""
        parts = (('studies', self.study_uid), ('series', self.series_uid),
                 ('instances', self.instance_uid))
        dicomweb_suffix = '/'.join(
            f'{part}/{part_uid}' for part, part_uid in parts
            if part_uid is not None)
        # Remove the trailing slash in case the suffix is empty.
        return f'{self.service_url}/{dicomweb_suffix}'.rstrip('/')

    @property
    def type(self) -> Type:
        """Type of the DICOM resource corresponding to the path."""
        if self.study_uid is None:
            return Type.SERVICE
        elif self.series_uid is None:
            return Type.STUDY
        elif self.instance_uid is None:
            return Type.SERIES
        return Type.INSTANCE

    def get_service_path(self) -> 'Path':
        """Returns the sub-path for the DICOMweb service within this path."""
        return Path(self.service_url)

    def get_study_path(self) -> 'Path':
        """Returns the sub-path for the DICOM Study within this path."""
        if self.type == Type.SERVICE:
            raise ValueError('Cannot get a study path from a service path.')
        return Path(self.service_url, self.study_uid)

    def get_series_path(self) -> 'Path':
        """Returns the sub-path for the DICOM Series within this path."""
        if self.type in (Type.SERVICE, Type.STUDY):
            raise ValueError(
                f'Cannot get a series path from a {self.type} path.')
        return Path(self.service_url, self.study_uid, self.series_uid)

    @classmethod
    def from_string(cls,
                    dicomweb_url: str,
                    path_type: Optional[Type] = None) -> 'Path':
        """Parses the string to return the Path.

        Any valid DICOMweb compatible HTTPS URL is permitted, e.g.,
        '<SERVICE>/studies/<StudyInstanceUID>/series/<SeriesInstanceUID>'

        Parameters
        ----------
        dicomweb_url: str
            An HTTPS DICOMweb-compatible URL.
        path_type: Type
            The expected type of the path.

        Returns
        -------
        The newly constructed *Path* object.

        Raises
        ------
        ValueError:
            If the path cannot be parsed or the actual path type doesn't match
            the specified expected *path_type*.
        """
        study_uid, series_uid, instance_uid = None, None, None
        # The URL format validation will happen when *Path* is returned at the
        # end.
        service_url_and_suffix = dicomweb_url.split('/studies/', maxsplit=1)
        service_url = service_url_and_suffix[0]

        if len(service_url_and_suffix) > 1:
            dicomweb_suffix = f'studies/{service_url_and_suffix[1]}'
            parts = dicomweb_suffix.split('/')
            while parts:
                part = parts.pop(0)
                if part == 'studies' and parts:
                    study_uid = parts.pop(0)
                elif part == 'series' and study_uid is not None and parts:
                    series_uid = parts.pop(0)
                elif part == 'instances' and series_uid is not None and parts:
                    instance_uid = parts.pop(0)
                else:
                    raise ValueError(
                        f'Error parsing the suffix "{dicomweb_suffix}" from '
                        f'URL: {dicomweb_url}')

        path = cls(service_url, study_uid, series_uid, instance_uid)
        # Validate that the path is of the right type of the type is specified.
        if path_type is not None and path.type != path_type:
            raise ValueError(
                f'Unexpected path type. Expected: {path_type}, Actual: '
                f'{path.type}. Path: {dicomweb_url}')

        return path
