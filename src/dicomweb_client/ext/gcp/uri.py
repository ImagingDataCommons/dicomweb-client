"""Utilities for Google Cloud Healthcare DICOMweb API URI manipulation.

For details, visit: https://cloud.google.com/healthcare
"""
import dataclasses
import re


# Used for Project ID and Location validation in `GoogleCloudHealthcareURL`.
_REGEX_ID_1 = re.compile(r'[\w-]+')
# Used for Dataset ID and DICOM Store ID validation in
# `GoogleCloudHealthcareURL`.
_REGEX_ID_2 = re.compile(r'[\w.-]+')
# Regex for the DICOM Store suffix for the Google Cloud Healthcare API endpoint.
_STORE_REGEX = re.compile(
    (r'projects/(%s)/locations/(%s)/datasets/(%s)/'
     r'dicomStores/(%s)/dicomWeb$') % (_REGEX_ID_1.pattern,
                                       _REGEX_ID_1.pattern,
                                       _REGEX_ID_2.pattern,
                                       _REGEX_ID_2.pattern))
# The URL for the Google Cloud Healthcare API endpoint.
_CHC_API_URL = 'https://healthcare.googleapis.com/v1'
# GCP resource name validation error.
_GCP_RESOURCE_ERROR_TMPL = ('`{attribute}` must match regex {regex}. Actual '
                            'value: {value!r}')


@dataclasses.dataclass(eq=True, frozen=True)
class GoogleCloudHealthcareURL:
    """Base URL container for DICOM Stores under the `Google Cloud Healthcare API`_.

    This class facilitates the parsing and creation of :py:attr:`URI.base_url`
    corresponding to DICOMweb API Service URLs under the v1_ API. The URLs are
    of the form:
    ``https://healthcare.googleapis.com/v1/projects/{project_id}/locations/{location}/datasets/{dataset_id}/dicomStores/{dicom_store_id}/dicomWeb``

    .. _Google Cloud Healthcare API: https://cloud.google.com/healthcare
    .. _v1: https://cloud.google.com/healthcare/docs/how-tos/transition-guide

    Attributes:
        project_id: str
            The ID of the `GCP Project
            <https://cloud.google.com/healthcare/docs/concepts/projects-datasets-data-stores#projects>`_
            that contains the DICOM Store.
        location: str
            The `Region name
            <https://cloud.google.com/healthcare/docs/concepts/regions>`_ of the
            geographic location configured for the Dataset that contains the
            DICOM Store.
        dataset_id: str
            The ID of the `Dataset
            <https://cloud.google.com/healthcare/docs/concepts/projects-datasets-data-stores#datasets_and_data_stores>`_
            that contains the DICOM Store.
        dicom_store_id: str
            The ID of the `DICOM Store
            <https://cloud.google.com/healthcare/docs/concepts/dicom#dicom_stores>`_.
    """
    project_id: str
    location: str
    dataset_id: str
    dicom_store_id: str

    def __post_init__(self) -> None:
        """Performs input sanity checks."""
        for regex, attribute, value in (
                (_REGEX_ID_1, 'project_id', self.project_id),
                (_REGEX_ID_1, 'location', self.location),
                (_REGEX_ID_2, 'dataset_id', self.dataset_id),
                (_REGEX_ID_2, 'dicom_store_id', self.dicom_store_id)):
            if regex.fullmatch(value) is None:
                raise ValueError(_GCP_RESOURCE_ERROR_TMPL.format(
                    attribute=attribute, regex=regex, value=value))

    def __str__(self) -> str:
        """Returns a string URL for use as :py:attr:`URI.base_url`.

        See class docstring for the returned URL format.
        """
        return (f'{_CHC_API_URL}/'
                f'projects/{self.project_id}/'
                f'locations/{self.location}/'
                f'datasets/{self.dataset_id}/'
                f'dicomStores/{self.dicom_store_id}/dicomWeb')

    @classmethod
    def from_string(cls, base_url: str) -> 'GoogleCloudHealthcareURL':
        """Creates an instance from ``base_url``.

        Parameters
        ----------
        base_url: str
            The URL for the DICOMweb API Service endpoint corresponding to a
            `CHC API DICOM Store
            <https://cloud.google.com/healthcare/docs/concepts/dicom#dicom_stores>`_.
            See class docstring for supported formats.

        Raises
        ------
        ValueError
            If ``base_url`` does not match the specifications in the class
            docstring.
        """
        if not base_url.startswith(f'{_CHC_API_URL}/'):
            raise ValueError('Invalid CHC API v1 URL: {base_url!r}')
        resource_suffix = base_url[len(_CHC_API_URL) + 1:]

        store_match = _STORE_REGEX.match(resource_suffix)
        if store_match is None:
            raise ValueError(
                'Invalid CHC API v1 DICOM Store name: {resource_suffix!r}')

        return cls(store_match.group(1), store_match.group(2),
                   store_match.group(3), store_match.group(4))
