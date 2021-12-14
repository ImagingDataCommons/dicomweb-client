import sys
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)
if sys.version_info.minor < 8:
    from typing_extensions import Protocol, runtime_checkable
else:
    from typing import Protocol, runtime_checkable

from pydicom.dataset import Dataset


@runtime_checkable
class DICOMClient(Protocol):

    """Protocol for DICOM clients based on DICOMweb interface."""

    def search_for_studies(
        self,
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None
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

        Returns
        -------
        List[Dict[str, dict]]
            Study representations
            (see `Study Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2>`_)

        Note
        ----
        The server may only return a subset of search results. In this case,
        a warning will notify the client that there are remaining results.
        Remaining results can be requested via repeated calls using the
        `offset` parameter.

        """  # noqa: E501: E501
        pass

    def retrieve_bulkdata(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        byte_range: Optional[Tuple[int, int]] = None
    ) -> List[bytes]:
        """Retrieve bulk data at a given location.

        Parameters
        ----------
        url: str
            Location of the bulk data
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        byte_range: Union[Tuple[int, int], None], optional
            Start and end of byte range

        Returns
        -------
        List[bytes]
            Bulk data items

        """
        pass

    def iter_bulkdata(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        byte_range: Optional[Tuple[int, int]] = None
    ) -> Iterator[bytes]:
        """Iterate over bulk data items at a given location.

        Parameters
        ----------
        url: str
            Location of the bulk data
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        byte_range: Union[Tuple[int, int], None], optional
            Start and end of byte range

        Returns
        -------
        Iterator[bytes]
            Bulk data items

        """
        pass

    def retrieve_study(
        self,
        study_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
    ) -> List[Dataset]:
        """Retrieve all instances of a study.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            acceptable transfer syntaxes

        Returns
        -------
        List[pydicom.dataset.Dataset]
            Instances

        Note
        ----
        Instances are by default retrieved using Implicit VR Little Endian
        transfer syntax (Transfer Syntax UID ``"1.2.840.10008.1.2"``). This
        means that Pixel Data of Image instances will be retrieved
        uncompressed. To retrieve instances in any available transfer syntax
        (typically the one in which instances were originally stored), specify
        acceptable transfer syntaxes using the wildcard
        ``("application/dicom", "*")``.

        """
        pass

    def iter_study(
        self,
        study_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
    ) -> Iterator[Dataset]:
        """Iterate over all instances of a study.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            acceptable transfer syntaxes

        Returns
        -------
        Iterator[pydicom.dataset.Dataset]
            Instances

        Note
        ----
        Instances are by default retrieved using Implicit VR Little Endian
        transfer syntax (Transfer Syntax UID ``"1.2.840.10008.1.2"``). This
        means that Pixel Data of Image instances will be retrieved
        uncompressed. To retrieve instances in any available transfer syntax
        (typically the one in which instances were originally stored), specify
        acceptable transfer syntaxes using the wildcard
        ``("application/dicom", "*")``.

        """
        pass

    def retrieve_study_metadata(
        self,
        study_instance_uid: str
    ) -> List[Dict[str, dict]]:
        """Retrieve metadata of all instances of a study.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID

        Returns
        -------
        List[Dict[str, dict]]
            Metadata of instances in DICOM JSON format

        """
        pass

    def delete_study(self, study_instance_uid: str) -> None:
        """Delete all instances of a study.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID

        Returns
        -------
        requests.models.Response
            HTTP response object returned.

        Note
        ----
        The Delete Study resource is not part of the DICOM standard
        and may not be supported by all origin servers.

        """
        pass

    def search_for_series(
        self,
        study_instance_uid: Optional[str] = None,
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None
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
        fields: Union[Union[list, tuple, set], None], optional
            Names of fields (attributes) that should be included in results
        search_filters: Union[Dict[str, Union[str, int, float]], None], optional
            Search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match

        Returns
        -------
        List[Dict[str, dict]]
            Series representations
            (see `Series Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2a>`_)

        Note
        ----
        The server may only return a subset of search results. In this case,
        a warning will notify the client that there are remaining results.
        Remaining results can be requested via repeated calls using the
        `offset` parameter.

        """  # noqa: E501
        pass

    def retrieve_series(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None
    ) -> List[Dataset]:
        """Retrieve all instances of a series.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            acceptable transfer syntaxes

        Returns
        -------
        List[pydicom.dataset.Dataset]
            Instances

        Note
        ----
        Instances are by default retrieved using Implicit VR Little Endian
        transfer syntax (Transfer Syntax UID ``"1.2.840.10008.1.2"``). This
        means that Pixel Data of Image instances will be retrieved
        uncompressed. To retrieve instances in any available transfer syntax
        (typically the one in which instances were originally stored), specify
        acceptable transfer syntaxes using the wildcard
        ``("application/dicom", "*")``.

        """
        pass

    def iter_series(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None
    ) -> Iterator[Dataset]:
        """Iterate over all instances of a series.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            acceptable transfer syntaxes

        Returns
        -------
        Iterator[pydicom.dataset.Dataset]
            Instances

        Note
        ----
        Instances are by default retrieved using Implicit VR Little Endian
        transfer syntax (Transfer Syntax UID ``"1.2.840.10008.1.2"``). This
        means that Pixel Data of Image instances will be retrieved
        uncompressed. To retrieve instances in any available transfer syntax
        (typically the one in which instances were originally stored), specify
        acceptable transfer syntaxes using the wildcard
        ``("application/dicom", "*")``.

        """
        pass

    def retrieve_series_metadata(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
    ) -> List[Dict[str, dict]]:
        """Retrieve metadata for all instances of a series.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID

        Returns
        -------
        Dict[str, dict]
            Metadata of instances in DICOM JSON format

        """
        pass

    def retrieve_series_rendered(
        self, study_instance_uid,
        series_instance_uid,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> bytes:
        """Retrieve rendered representation of a series.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
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

        """
        pass

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

        Returns
        -------
        requests.models.Response
            HTTP response object returned.

        Note
        ----
        The Delete Series resource is not part of the DICOM standard
        and may not be supported by all origin servers.

        """
        pass

    def search_for_instances(
        self,
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None,
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None
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
        fields: Union[Union[list, tuple, set], None], optional
            Names of fields (attributes) that should be included in results
        search_filters: Union[Dict[str, Union[str, int, float]], None], optional
            Search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match

        Returns
        -------
        List[Dict[str, dict]]
            Instance representations
            (see `Instance Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2b>`_)

        Note
        ----
        The server may only return a subset of search results. In this case,
        a warning will notify the client that there are remaining results.
        Remaining results can be requested via repeated calls using the
        `offset` parameter.

        """  # noqa: E501
        pass

    def retrieve_instance(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
    ) -> Dataset:
        """Retrieve an individual instance.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        sop_instance_uid: str
            SOP Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            acceptable transfer syntaxes

        Returns
        -------
        pydicom.dataset.Dataset
            Instance

        Note
        ----
        Instances are by default retrieved using Implicit VR Little Endian
        transfer syntax (Transfer Syntax UID ``"1.2.840.10008.1.2"``). This
        means that Pixel Data of Image instances will be retrieved
        uncompressed. To retrieve instances in any available transfer syntax
        (typically the one in which instances were originally stored), specify
        acceptable transfer syntaxes using the wildcard
        ``("application/dicom", "*")``.

        """
        pass

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
        pass

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

        Returns
        -------
        requests.models.Response
            HTTP response object returned.

        Note
        ----
        The Delete Instance resource is not part of the DICOM standard
        and may not be supported by all origin servers.

        """
        pass

    def retrieve_instance_metadata(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str
    ) -> Dict[str, dict]:
        """Retrieve metadata of an individual instance.

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
        Dict[str, dict]
            Metadata of instance in DICOM JSON format

        """
        pass

    def retrieve_instance_rendered(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
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
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
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

        """
        pass

    def retrieve_instance_frames(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        frame_numbers: Sequence[int],
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None
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
        frame_numbers: Sequence[int]
            One-based positional indices of the frames within the instance
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        List[bytes]
            Pixel data for each frame

        """
        pass

    def iter_instance_frames(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        frame_numbers: Sequence[int],
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None
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
        frame_numbers: Sequence[int]
            One-based positional indices of the frames within the instance
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        Iterator[bytes]
            Pixel data for each frame

        """
        pass

    def retrieve_instance_frames_rendered(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        frame_numbers: Sequence[int],
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        params: Optional[Dict[str, Any]] = None
    ) -> bytes:
        """Retrieve one or more server-side rendered frames of an instance.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        sop_instance_uid: str
            SOP Instance UID
        frame_numbers: Sequence[int]
            One-based positional index of the frame within the instance
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media type (choices: ``"image/jpeg"``, ``"image/jp2"``,
            ``"image/gif"``, ``"image/png"``)
        params: Union[Dict[str, Any], None], optional
            Additional parameters relevant for given `media_type`,
            e.g., ``{"quality": 95}`` for ``"image/jpeg"`` media type

        Returns
        -------
        bytes
            Rendered representation of frames

        Note
        ----
        Not all media types are compatible with all SOP classes.

        """
        pass
