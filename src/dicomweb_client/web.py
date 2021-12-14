"""Client for interacting with a DICOMweb service over network using HTTP.

Facilitates access to data stored remotely on a server.

"""
import re
import logging
from enum import Enum
from collections import OrderedDict
from http import HTTPStatus
from io import BytesIO
from typing import (
    Any,
    Callable,
    Dict,
    Iterator,
    List,
    Optional,
    Set,
    Sequence,
    Union,
    Tuple,
)
from urllib.parse import quote_plus, urlparse
from warnings import warn
from xml.etree.ElementTree import (
    Element,
    fromstring
)

import pydicom
import requests
import retrying


logger = logging.getLogger(__name__)


def _load_xml_dataset(dataset: Element) -> pydicom.dataset.Dataset:
    """Load DICOM Data Set in DICOM XML format.

    Parameters
    ----------
    dataset: xml.etree.ElementTree.Element
        parsed element tree

    Returns
    -------
    pydicom.dataset.Dataset
        data set

    """
    ds = pydicom.Dataset()
    for element in dataset:
        keyword = element.attrib['keyword']
        vr = element.attrib['vr']
        value: Optional[Union[List[Any], str]]
        if vr == 'SQ':
            value = [
                _load_xml_dataset(item)
                for item in element
            ]
        else:
            value = list(element)
            if len(value) == 1:
                value = value[0].text.strip()
            elif len(value) > 1:
                value = [v.text.strip() for v in value]
            else:
                value = None
        setattr(ds, keyword, value)
    return ds


class _Transaction(Enum):

    STORE = 'store'
    SEARCH = 'search'
    RETRIEVE = 'retrieve'
    DELETE = 'delete'


class DICOMwebClient:

    """Class for connecting to and interacting with a DICOMweb RESTful service.

    Attributes
    ----------
    base_url: str
        Unique resource locator of the DICOMweb service
    protocol: str
        Name of the protocol, e.g. ``"https"``
    host: str
        IP address or DNS name of the machine that hosts the server
    port: int
        Number of the port to which the server listens
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
    chunk_size: int
        Maximum number of bytes that should be transferred per data chunk
        when streaming data from the server using chunked transfer encoding
        (used by ``iter_*()`` methods as well as the ``store_instances()``
        method)

    """

    def set_chunk_size(self, chunk_size: int) -> None:
        """Set value of `chunk_size` attribute.

        Parameters
        ----------
        chunk_size: int
            Maximum number of bytes that should be transferred per data chunk
            when streaming data from the server using chunked transfer encoding
            (used by ``iter_*()`` methods as well as the ``store_instances()``
            method)

        """
        self._chunk_size = chunk_size

    def set_http_retry_params(
        self,
        retry: bool = True,
        max_attempts: int = 5,
        wait_exponential_multiplier: int = 1000,
        retriable_error_codes: Tuple[HTTPStatus, ...] = (
            HTTPStatus.TOO_MANY_REQUESTS,
            HTTPStatus.REQUEST_TIMEOUT,
            HTTPStatus.SERVICE_UNAVAILABLE,
            HTTPStatus.GATEWAY_TIMEOUT,
        )
    ) -> None:
        """Set parameters for HTTP retrying logic.

        These parameters determine whether and how individual HTTP requests
        will be retried in case the origin server responds with an error code
        defined in `retriable_error_codes`.
        The retrying method uses exponential back off using the multiplier
        `wait_exponential_multiplier` for a max attempts defined by
        `max_attempts`.

        Parameters
        ----------
        retry: bool, optional
            Whether HTTP retrying should be performed, if it is set to
            ``False``, the rest of the parameters are ignored.
        max_attempts: int, optional
            The maximum number of request attempts.
        wait_exponential_multiplier: float, optional
            Exponential multiplier applied to delay between attempts in ms.
        retriable_error_codes: tuple, optional
            Tuple of HTTP error codes to retry if raised.

        """
        self._http_retry = retry
        if retry:
            self._max_attempts = max_attempts
            self._wait_exponential_multiplier = wait_exponential_multiplier
            self._http_retrable_errors = retriable_error_codes

        else:
            self._max_attempts = 1
            self._wait_exponential_multiplier = 1
            self._http_retrable_errors = ()

    def _is_retriable_http_error(
        self,
        response: requests.models.Response
    ) -> bool:
        """Determine whether the given response's status code is retriable.

        Parameters
        ----------
        response: requests.models.Response
            HTTP response object returned by the request method.

        Returns
        -------
        bool
            Whether the HTTP request should be retried.

        """
        return response.status_code in self._http_retrable_errors

    def __init__(
        self,
        url: str,
        session: Optional[requests.Session] = None,
        qido_url_prefix: Optional[str] = None,
        wado_url_prefix: Optional[str] = None,
        stow_url_prefix: Optional[str] = None,
        delete_url_prefix: Optional[str] = None,
        proxies: Optional[Dict[str, str]] = None,
        headers: Optional[Dict[str, str]] = None,
        callback: Optional[Callable] = None,
        chunk_size: int = 10**6
    ) -> None:
        """Instatiate client.

        Parameters
        ----------
        url: str
            Unique resource locator of the DICOMweb service consisting of
            protocol, hostname (IP address or DNS name) of the machine that
            hosts the service and optionally port number and path prefix
        session: Union[requests.Session, None], optional
            Session required to make connections to the DICOMweb service
            (see ``dicomweb_client.session_utils`` module to create a valid
            session if necessary)
        qido_url_prefix: Union[str, None], optional
            URL path prefix for QIDO RESTful services
        wado_url_prefix: Union[str, None], optional
            URL path prefix for WADO RESTful services
        stow_url_prefix: Union[str, None], optional
            URL path prefix for STOW RESTful services
        delete_url_prefix: Union[str, None], optional
            URL path prefix for DELETE RESTful services
        proxies: Union[Dict[str, str], None], optional
            Mapping of protocol or protocol + host to the URL of a proxy server
        headers: Union[Dict[str, str], None], optional
            Custom headers that should be included in request messages,
            e.g., authentication tokens
        callback: Union[Callable[[requests.Response, ...], requests.Response], None], optional
            Callback function to manipulate responses generated from requests
            (see `requests event hooks <http://docs.python-requests.org/en/master/user/advanced/#event-hooks>`_)
        chunk_size: int, optional
            Maximum number of bytes that should be transferred per data chunk
            when streaming data from the server using chunked transfer encoding
            (used by ``iter_*()`` methods as well as the ``store_instances()``
            method); defaults to ``10**6`` bytes (10MB)

        Warning
        -------
        Modifies the passed `session` (in particular header fields),
        so be careful when reusing the session outside the scope of an instance.

        Warning
        -------
        Choose the value of `chunk_size` carefully. A small value may cause
        significant network communication and message parsing overhead.

        """  # noqa: E501
        if session is None:
            logger.debug('initialize HTTP session')
            session = requests.session()
        self._session = session
        self.base_url = url
        self.qido_url_prefix = qido_url_prefix
        self.wado_url_prefix = wado_url_prefix
        self.stow_url_prefix = stow_url_prefix
        self.delete_url_prefix = delete_url_prefix

        # This regular expression extracts the scheme and host name from the URL
        # and optionally the port number and prefix:
        # <scheme>://<host>(:<port>)(/<prefix>)
        # For example: "https://mydomain.com:443/wado-rs", where
        # scheme="https", host="mydomain.com", port=443, prefix="wado-rs"
        pattern = re.compile(
            r'(?P<scheme>[https]+)://(?P<host>[^/:]+)'
            r'(?::(?P<port>\d+))?(?:(?P<prefix>/[\w/]+))?'
        )
        match = re.match(pattern, self.base_url)
        if match is None:
            raise ValueError(f'Malformed URL: {self.base_url}')
        try:
            self.protocol = match.group('scheme')
            self.host = match.group('host')
            port = match.group('port')
        except AttributeError:
            raise ValueError(f'Malformed URL: {self.base_url}')
        if port:
            self.port = int(port)
        else:
            if self.protocol == 'http':
                self.port = 80
            elif self.protocol == 'https':
                self.port = 443
            else:
                raise ValueError(
                    f'URL scheme "{self.protocol}" is not supported.'
                )
        url_components = urlparse(url)
        self.url_prefix = url_components.path
        if headers is not None:
            self._session.headers.update(headers)
        if proxies is not None:
            self._session.proxies = proxies
        if callback is not None:
            self._session.hooks = {'response': [callback, ]}
        self._chunk_size = chunk_size
        self.set_http_retry_params()

    def _parse_qido_query_parameters(
        self,
        fuzzymatching: Optional[bool] = None,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        fields: Optional[Sequence[str]] = None,
        search_filters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Parse query parameters for inclusion into a query string.

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
        search_filters: Union[Dict[str, Any], None], optional
            Search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match

        Returns
        -------
        collections.OrderedDict
            Sanitized and sorted query parameters

        """
        params: Dict[str, Union[int, str, List[str]]] = {}
        if limit is not None:
            if not(isinstance(limit, int)):
                raise TypeError('Parameter "limit" must be an integer.')
            if limit < 0:
                raise ValueError('Parameter "limit" must not be negative.')
            params['limit'] = limit
        if offset is not None:
            if not(isinstance(offset, int)):
                raise TypeError('Parameter "offset" must be an integer.')
            if offset < 0:
                raise ValueError('Parameter "offset" must not be negative.')
            params['offset'] = offset
        if fuzzymatching is not None:
            if not(isinstance(fuzzymatching, bool)):
                raise TypeError('Parameter "fuzzymatching" must be boolean.')
            if fuzzymatching:
                params['fuzzymatching'] = 'true'
            else:
                params['fuzzymatching'] = 'false'
        if fields is not None:
            includefields = []
            for field in set(fields):
                if not(isinstance(field, str)):
                    raise TypeError('Elements of "fields" must be a string.')
                includefields.append(field)
            params['includefield'] = includefields
        if search_filters is not None:
            for field, criterion in search_filters.items():
                if not(isinstance(field, str)):
                    raise TypeError(
                        'Keys of "search_filters" must be strings.'
                    )
                # TODO: datetime?
                params[field] = criterion
        # Sort query parameters to facilitate unit testing
        return OrderedDict(sorted(params.items()))

    def _get_transaction_url(self, transaction: _Transaction) -> str:
        """Construct URL of a DICOMweb service transaction.

        Parameters
        ----------
        transaction: dicomweb_client.api._Transaction
            Type of transaction

        Returns
        -------
        str
            Full URL for the given transaction

        """
        transaction_url = self.base_url
        if transaction == _Transaction.SEARCH:
            if self.qido_url_prefix is not None:
                transaction_url += f'/{self.qido_url_prefix}'
        elif transaction == _Transaction.RETRIEVE:
            if self.wado_url_prefix is not None:
                transaction_url += f'/{self.wado_url_prefix}'
        elif transaction == _Transaction.STORE:
            if self.stow_url_prefix is not None:
                transaction_url += f'/{self.stow_url_prefix}'
        elif transaction == _Transaction.DELETE:
            if self.delete_url_prefix is not None:
                transaction_url += f'/{self.delete_url_prefix}'
        else:
            raise ValueError(
                f'Unsupported DICOMweb service "{transaction.value}".'
            )
        return transaction_url

    def _get_studies_url(
        self,
        transaction: _Transaction,
        study_instance_uid: Optional[str] = None
    ) -> str:
        """Construct URL for study-level requests.

        Parameters
        ----------
        transaction: dicomweb_client.api._Transaction
            Type of transaction
        study_instance_uid: Union[str, None], optional
            Study Instance UID

        Returns
        -------
        str
            URL

        """
        if study_instance_uid is not None:
            url = '{transaction_url}/studies/{study_instance_uid}'
        else:
            url = '{transaction_url}/studies'
        transaction_url = self._get_transaction_url(transaction)
        return url.format(
            transaction_url=transaction_url,
            study_instance_uid=study_instance_uid
        )

    def _get_series_url(
        self,
        transaction: _Transaction,
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None
    ) -> str:
        """Construct URL for series-level requests.

        Parameters
        ----------
        transaction: dicomweb_client.api._Transaction
            Type of transaction
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
            url = self._get_studies_url(transaction, study_instance_uid)
            if series_instance_uid is not None:
                url += '/series/{series_instance_uid}'
            else:
                url += '/series'
        else:
            if series_instance_uid is not None:
                logger.warning(
                    'series UID is ignored because study UID is undefined'
                )
            url = '{transaction_url}/series'
        transaction_url = self._get_transaction_url(transaction)
        return url.format(
            transaction_url=transaction_url,
            series_instance_uid=series_instance_uid
        )

    def _get_instances_url(
        self,
        transaction: _Transaction,
        study_instance_uid: Optional[str] = None,
        series_instance_uid: Optional[str] = None,
        sop_instance_uid: Optional[str] = None
    ) -> str:
        """Construct URL for instance-level requests.

        Parameters
        ----------
        transaction: dicomweb_client.api._Transaction
            Type of transaction
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
            url = self._get_series_url(
                transaction,
                study_instance_uid,
                series_instance_uid
            )
            url += '/instances'
            if sop_instance_uid is not None:
                url += '/{sop_instance_uid}'
        elif study_instance_uid is not None:
            if sop_instance_uid is not None:
                logger.warning(
                    'SOP Instance UID is ignored because Series Instance UID '
                    'is undefined'
                )
            url = self._get_studies_url(
                transaction,
                study_instance_uid
            )
            url += '/instances'
        else:
            if sop_instance_uid is not None:
                logger.warning(
                    'SOP Instance UID is ignored because Study/Series '
                    'Instance UID are undefined'
                )
            url = '{transaction_url}/instances'
        transaction_url = self._get_transaction_url(transaction)
        return url.format(
            transaction_url=transaction_url,
            sop_instance_uid=sop_instance_uid
        )

    def _build_query_string(self, params: Dict[str, Any]) -> str:
        """Build query string for a request message.

        Parameters
        ----------
        params: dict
            Query parameters as mapping of key-value pairs;
            in case a key should be included more than once with different
            values, values need to be provided in form of an iterable (e.g.,
            ``{"key": ["value1", "value2"]}`` will result in
            ``"?key=value1&key=value2"``)

        Returns
        -------
        str
            Query string

        """
        components = []
        for key, value in params.items():
            if isinstance(value, (list, tuple, set)):
                for v in value:
                    c = '='.join([key, quote_plus(str(v))])
                    components.append(c)
            else:
                c = '='.join([key, quote_plus(str(value))])
                components.append(c)
        if components:
            return '?{}'.format('&'.join(components))
        else:
            return ''

    def _http_get(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        stream: bool = False
    ) -> requests.models.Response:
        """Perform an HTTP GET request.

        Parameters
        ----------
        url: str
            Unique resource locator
        params: Union[Dict[str, Any], None], optional
            Query parameters
        headers: Union[Dict[str, str], None], optional
            Request message headers
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        requests.models.Response
            Response message

        """
        @retrying.retry(
            retry_on_result=self._is_retriable_http_error,
            wait_exponential_multiplier=self._wait_exponential_multiplier,
            stop_max_attempt_number=self._max_attempts
        )
        def _invoke_get_request(
            url: str,
            headers: Optional[Dict[str, str]] = None
        ) -> requests.models.Response:
            logger.debug(f'GET: {url} {headers}')
            # Setting stream allows for retrieval of data using chunked transer
            # encoding. The iter_content() method can be used to iterate over
            # chunks. If stream is not set, iter_content() will return the
            # full payload at once.
            return self._session.get(url=url, headers=headers, stream=stream)

        if headers is None:
            headers = {}
        if not stream:
            headers['Host'] = self.host
        if params is None:
            params = {}
        url += self._build_query_string(params)
        if stream:
            logger.debug('use chunked transfer encoding')
        response = _invoke_get_request(url, headers)
        logger.debug(f'request status code: {response.status_code}')
        response.raise_for_status()
        if response.status_code == 204:
            logger.warning('empty response')
        # The server may not return all results, but rather include a warning
        # header to notify that client that there are remaining results.
        # (see DICOM Part 3.18 Section 6.7.1.2)
        if 'Warning' in response.headers:
            logger.warning(response.headers['Warning'])
        return response

    def _http_get_application_json(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        stream: bool = False
    ) -> List[Dict[str, dict]]:
        """GET a resource with "applicaton/dicom+json" media type.

        Parameters
        ----------
        url: str
            Unique resource locator
        params: Union[Dict[str, Any], None], optional
            Query parameters
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        List[str, dict]
            Content of HTTP message body in DICOM JSON format

        """
        content_type = 'application/dicom+json, application/json'
        response = self._http_get(
            url,
            params=params,
            headers={'Accept': content_type},
            stream=stream
        )
        if response.content:
            decoded_response = response.json()
            # All metadata resources are expected to be sent as a JSON array of
            # DICOM data sets. However, some origin servers may incorrectly
            # sent an individual data set.
            if isinstance(decoded_response, dict):
                return [decoded_response]
            return decoded_response
        return []

    @classmethod
    def _extract_part_content(cls, part: bytes) -> Union[bytes, None]:
        """Extract the content of a single part of a multipart response message.

        Parameters
        ----------
        part: bytes
            Individual part of a multipart message

        Returns
        -------
        Union[bytes, None]
            Content of the message part or ``None`` in case the message
            part is empty

        Raises
        ------
        ValueError
            When the message part is not CRLF CRLF terminated

        """
        if part in (b'', b'--', b'\r\n') or part.startswith(b'--\r\n'):
            return None
        idx = part.index(b'\r\n\r\n')
        if idx > -1:
            return part[idx + 4:]
        raise ValueError('Message part does not contain CRLF CRLF')

    def _decode_multipart_message(
        self,
        response: requests.Response,
        stream: bool
    ) -> Iterator[bytes]:
        """Decode extracted parts of a multipart response message.

        Parameters
        ----------
        response: requests.Response
            Response message
        stream: bool
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        Iterator[bytes]
            Message parts

        """
        logger.debug('decode multipart message')
        logger.debug('decode message header')
        content_type = response.headers['content-type']
        media_type, *ct_info = [ct.strip() for ct in content_type.split(';')]
        if media_type.lower() != 'multipart/related':
            raise ValueError(
                f'Unexpected media type: "{media_type}". '
                'Expected "multipart/related".'
            )
        for item in ct_info:
            attr, _, value = item.partition('=')
            if attr.lower() == 'boundary':
                boundary = value.strip('"').encode('utf-8')
                break
        else:
            # Some servers set the media type to multipart but don't provide a
            # boundary and just send a single frame in the body - return as is.
            yield response.content
            return

        marker = b''.join((b'--', boundary))
        delimiter = b''.join((b'\r\n', marker))
        data = b''
        j = 0
        with response:
            logger.debug('decode message content')
            if stream:
                iterator = response.iter_content(chunk_size=self._chunk_size)
            else:
                iterator = iter([response.content])
            for i, chunk in enumerate(iterator):
                if stream:
                    logger.debug(f'decode message content chunk #{i}')
                data += chunk
                while delimiter in data:
                    logger.debug(f'decode message part #{j}')
                    part, data = data.split(delimiter, maxsplit=1)
                    content = self._extract_part_content(part)
                    j += 1
                    if content is not None:
                        logger.debug(
                            f'extracted {len(content)} bytes from part #{j}'
                        )
                        yield content

        content = self._extract_part_content(data)
        if content is not None:
            yield content

    @classmethod
    def _encode_multipart_message(
        cls,
        content: Sequence[bytes],
        content_type: str
    ) -> bytes:
        """Encode the payload of a multipart response message.

        Parameters
        ----------
        content: Sequence[bytes]
            Content of each part
        content_type: str
            Content type of the multipart HTTP request message

        Returns
        -------
        bytes
            HTTP request message body

        """
        media_type, *ct_info = [ct.strip() for ct in content_type.split(';')]
        if media_type != 'multipart/related':
            raise ValueError(
                'No "multipart/related" usage found in content type field'
            )
        parameters = {}
        for item in ct_info:
            name, value = item.split('=')
            parameters[name.lower()] = value.strip('"')
        try:
            content_type = parameters['type']
        except KeyError:
            raise ValueError(
                'No "type" parameter in found in content-type field'
            )
        try:
            boundary = parameters['boundary']
        except KeyError:
            raise ValueError(
                'No "boundary" parameter in found in content-type field'
            )
        body = b''
        for part in content:
            body += f'\r\n--{boundary}'.encode('utf-8')
            body += f'\r\nContent-Type: {content_type}\r\n\r\n'.encode('utf-8')
            body += part
        body += f'\r\n--{boundary}--'.encode('utf-8')
        return body

    @classmethod
    def _assert_media_type_is_valid(cls, media_type: str):
        """Assert that a given media type is valid.

        Parameters
        ----------
        media_type: str
            Media type

        Raises
        ------
        ValueError
            When `media_type` is invalid

        """
        error_message = f'Not a valid media type: "{media_type}"'
        sep_index = media_type.find('/')
        if sep_index == -1:
            raise ValueError(error_message)
        media_type_type = media_type[:sep_index]
        if media_type_type not in {'application', 'image', 'text', 'video'}:
            raise ValueError(error_message)
        if media_type.find('/', sep_index + 1) > 0:
            raise ValueError(error_message)

    @classmethod
    def _build_range_header_field_value(
        cls,
        byte_range: Optional[Tuple[int, int]] = None
    ) -> str:
        """Build a range header field value for a request message.

        Parameters
        ----------
        byte_range: Union[Tuple[int, int], None], optional
            Start and end of byte range

        Returns
        -------
        str
            Range header field value

        """
        if byte_range is not None:
            start = str(byte_range[0])
            try:
                end = str(byte_range[1])
            except IndexError:
                end = ''
            range_header_field_value = f'bytes={start}-{end}'
        else:
            range_header_field_value = 'bytes=0-'
        return range_header_field_value

    @classmethod
    def _build_accept_header_field_value(
        cls,
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None],
        supported_media_types: Set[str]
    ) -> str:
        """Build an accept header field value for a request message.

        Parameters
        ----------
        media_types: Union[Tuple[str], None]
            Acceptable media types
        supported_media_types: Set[str]
            Supported media types

        Returns
        -------
        str
            Accept header field value

        """
        if not isinstance(media_types, (list, tuple, set)):
            raise TypeError(
                'Acceptable media types must be provided as a sequence.'
            )
        field_value_parts = []
        for media_type in media_types:
            if not isinstance(media_type, str):
                raise TypeError(
                    f'Media type "{media_type}" is not supported for '
                    'requested resource'
                )
            cls._assert_media_type_is_valid(media_type)
            if media_type not in supported_media_types:
                raise ValueError(
                    f'Media type "{media_type}" is not supported for '
                    'requested resource'
                )
            field_value_parts.append(media_type)
        return ', '.join(field_value_parts)

    @classmethod
    def _build_multipart_accept_header_field_value(
        cls,
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None],
        supported_media_types: Union[Dict[str, str], Set[str]]
    ) -> str:
        """Build an accept header field value for a multipart request message.

        Parameters
        ----------
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None]
            Acceptable media types and optionally the UIDs of the corresponding
            transfer syntaxes
        supported_media_types: Union[Dict[str, str], Set[str]]
            Set of supported media types or mapping of transfer syntaxes
            to their corresponding media types

        Returns
        -------
        str
            Accept header field value

        """
        if not isinstance(media_types, (list, tuple, set)):
            raise TypeError(
                'Acceptable media types must be provided as a sequence.'
            )
        field_value_parts = []
        for item in media_types:
            if isinstance(item, str):
                media_type = item
                transfer_syntax_uid = None
            else:
                media_type = item[0]
                try:
                    transfer_syntax_uid = item[1]
                except IndexError:
                    transfer_syntax_uid = None
            cls._assert_media_type_is_valid(media_type)
            field_value = f'multipart/related; type="{media_type}"'
            if isinstance(supported_media_types, dict):
                if media_type not in supported_media_types.values():
                    if not (media_type.endswith('/*') or
                            media_type.endswith('/')):
                        raise ValueError(
                            f'Media type "{media_type}" is not supported for '
                            'requested resource.'
                        )
                if transfer_syntax_uid is not None:
                    if transfer_syntax_uid != '*':
                        if transfer_syntax_uid not in supported_media_types:
                            raise ValueError(
                                f'Transfer syntax "{transfer_syntax_uid}" '
                                'is not supported for requested resource.'
                            )
                        expected_media_type = supported_media_types[
                            transfer_syntax_uid
                        ]
                        if expected_media_type != media_type:
                            have_same_type = (
                                cls._parse_media_type(media_type)[0] ==
                                cls._parse_media_type(expected_media_type)[0]
                            )
                            if (have_same_type and
                                    (media_type.endswith('/*') or
                                        media_type.endswith('/'))):
                                continue
                            raise ValueError(
                                f'Transfer syntax "{transfer_syntax_uid}" '
                                'is not supported for media '
                                f'type "{media_type}".'
                            )
                    field_value += f'; transfer-syntax={transfer_syntax_uid}'
            else:
                if media_type not in supported_media_types:
                    raise ValueError(
                        f'Media type "{media_type}" is not supported for '
                        'requested resource.'
                    )
            field_value_parts.append(field_value)
        return ', '.join(field_value_parts)

    def _http_get_multipart_application_dicom(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        params: Optional[Dict[str, Any]] = None,
        stream: bool = False
    ) -> Iterator[pydicom.dataset.Dataset]:
        """GET a multipart resource with "applicaton/dicom" media type.

        Parameters
        ----------
        url: str
            Unique resource locator
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes, (defaults to
            ``("application/dicom", "1.2.840.10008.1.2.1")``)
        params: Union[Dict[str, Any], None], optional
            Additional HTTP GET query parameters
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        Iterator[pydicom.dataset.Dataset]
            DICOM data sets

        """
        default_media_type = 'application/dicom'
        supported_media_types = {
            '1.2.840.10008.1.2.1': default_media_type,
            '1.2.840.10008.1.2.5': default_media_type,
            '1.2.840.10008.1.2.4.50': default_media_type,
            '1.2.840.10008.1.2.4.51': default_media_type,
            '1.2.840.10008.1.2.4.57': default_media_type,
            '1.2.840.10008.1.2.4.70': default_media_type,
            '1.2.840.10008.1.2.4.80': default_media_type,
            '1.2.840.10008.1.2.4.81': default_media_type,
            '1.2.840.10008.1.2.4.90': default_media_type,
            '1.2.840.10008.1.2.4.91': default_media_type,
            '1.2.840.10008.1.2.4.92': default_media_type,
            '1.2.840.10008.1.2.4.93': default_media_type,
            '1.2.840.10008.1.2.4.100': default_media_type,
            '1.2.840.10008.1.2.4.101': default_media_type,
            '1.2.840.10008.1.2.4.102': default_media_type,
            '1.2.840.10008.1.2.4.103': default_media_type,
            '1.2.840.10008.1.2.4.104': default_media_type,
            '1.2.840.10008.1.2.4.105': default_media_type,
            '1.2.840.10008.1.2.4.106': default_media_type,
        }
        if media_types is None:
            media_types = (default_media_type, )
        headers = {
            'Accept': self._build_multipart_accept_header_field_value(
                media_types, supported_media_types
            ),
        }
        response = self._http_get(
            url,
            params=params,
            headers=headers,
            stream=stream
        )
        # The response of the Retrieve Instance transaction is supposed to
        # contain a message body with Content-Type "multipart/related", even
        # if it only contains a single part. However, some origin servers
        # violate the standard and send the part non-encapsulated.
        # Unfortunately, an error was introduced into the standard via
        # Supplement 183 as part of re-documentation efforts, which stated that
        # this behavior was allowed. We will support this behavior at least
        # until the standard is fixed via a Correction Proposal 2040.
        if response.headers['Content-Type'].startswith('application/dicom'):
            warning_message = (
                'message sent by origin server in response to GET request '
                'of Retrieve Instance transaction was not compliant with the '
                'DICOM standard, message body shall have Content-Type '
                '\'multipart/related; type="application/dicom"\' rather than '
                '"application/dicom"'
            )
            warn(warning_message, category=UserWarning)
            part = pydicom.dcmread(BytesIO(response.content))
            return iter([part])
        return (
            pydicom.dcmread(BytesIO(part))
            for part in self._decode_multipart_message(response, stream=stream)
        )

    def _http_get_multipart_application_octet_stream(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        byte_range: Optional[Tuple[int, int]] = None,
        params: Optional[Dict[str, Any]] = None,
        stream: bool = False
    ) -> Iterator[bytes]:
        """GET a multipart resource with "applicaton/octet-stream" media type.

        Parameters
        ----------
        url: str
            Unique resource locator
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes (defaults to
            ``("application/octet-stream", "1.2.840.10008.1.2.1")``)
        byte_range: Union[Tuple[int, int], None], optional
            Start and end of byte range
        params: Union[Dict[str, Any], None], optional
            Additional HTTP GET query parameters
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        Iterator[bytes]
            Content of HTTP message body parts

        """
        default_media_type = 'application/octet-stream'
        supported_media_types = {
            '1.2.840.10008.1.2.1': default_media_type,
        }
        if media_types is None:
            media_types = (default_media_type, )
        headers = {
            'Accept': self._build_multipart_accept_header_field_value(
                media_types,
                supported_media_types
            ),
        }
        if byte_range is not None:
            headers['Range'] = self._build_range_header_field_value(byte_range)
        response = self._http_get(
            url,
            params=params,
            headers=headers,
            stream=stream
        )
        return self._decode_multipart_message(response, stream=stream)

    def _http_get_multipart_image(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        byte_range: Optional[Tuple[int, int]] = None,
        params: Optional[Dict[str, Any]] = None,
        rendered: bool = False,
        stream: bool = False
    ) -> Iterator[bytes]:
        """GET a multipart resource with "image/" media type.

        Parameters
        ----------
        url: str
            Unique resource locator
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        byte_range: Union[Tuple[int, int], None], optional
            Start and end of byte range
        params: Union[Dict[str, Any], None], optional
            Additional HTTP GET query parameters
        rendered: bool, optional
            Whether resource should be requested using rendered media types
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        Iterator[bytes]
            Content of HTTP message body parts

        """
        headers = {}
        supported_media_types: Union[set, dict]
        if rendered:
            supported_media_types = {
                'image/jpeg',
                'image/gif',
                'image/png',
                'image/jp2',
            }
        else:
            supported_media_types = {
                '1.2.840.10008.1.2.5': 'image/x-dicom-rle',
                '1.2.840.10008.1.2.4.50': 'image/jpeg',
                '1.2.840.10008.1.2.4.51': 'image/jpeg',
                '1.2.840.10008.1.2.4.57': 'image/jpeg',
                '1.2.840.10008.1.2.4.70': 'image/jpeg',
                '1.2.840.10008.1.2.4.80': 'image/x-jls',
                '1.2.840.10008.1.2.4.81': 'image/x-jls',
                '1.2.840.10008.1.2.4.90': 'image/jp2',
                '1.2.840.10008.1.2.4.91': 'image/jp2',
                '1.2.840.10008.1.2.4.92': 'image/jpx',
                '1.2.840.10008.1.2.4.93': 'image/jpx',
            }
            if byte_range is not None:
                headers['Range'] = self._build_range_header_field_value(
                    byte_range
                )
        headers['Accept'] = self._build_multipart_accept_header_field_value(
            media_types,
            supported_media_types
        )
        response = self._http_get(
            url,
            params=params,
            headers=headers,
            stream=stream
        )
        return self._decode_multipart_message(response, stream=stream)

    def _http_get_multipart_video(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        byte_range: Optional[Tuple[int, int]] = None,
        params: Optional[Dict[str, Any]] = None,
        rendered: bool = False,
        stream: bool = False
    ) -> Iterator[bytes]:
        """GET a multipart resource with "video/" media type.

        Parameters
        ----------
        url: str
            Unique resource locator
        media_types: Tuple[Union[str, Tuple[str, str]]]
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        byte_range: Union[Tuple[int, int], None], optional
            Start and end of byte range
        params: Union[Dict[str, Any], None], optional
            Additional HTTP GET query parameters
        rendered: bool, optional
            Whether resource should be requested using rendered media types
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        Iterator[bytes]
            Content of HTTP message body parts

        """
        headers = {}
        supported_media_types: Union[set, dict]
        if rendered:
            supported_media_types = {
                'video/',
                'video/*',
                'video/mpeg2',
                'video/mp4',
                'video/H265',
            }
        else:
            supported_media_types = {
                '1.2.840.10008.1.2.4.100': 'video/mpeg2',
                '1.2.840.10008.1.2.4.101': 'video/mpeg2',
                '1.2.840.10008.1.2.4.102': 'video/mp4',
                '1.2.840.10008.1.2.4.103': 'video/mp4',
                '1.2.840.10008.1.2.4.104': 'video/mp4',
                '1.2.840.10008.1.2.4.105': 'video/mp4',
                '1.2.840.10008.1.2.4.106': 'video/mp4',
            }
            if byte_range is not None:
                headers['Range'] = self._build_range_header_field_value(
                    byte_range
                )
        headers['Accept'] = self._build_multipart_accept_header_field_value(
            media_types,
            supported_media_types
        )
        response = self._http_get(
            url,
            params=params,
            headers=headers,
            stream=stream
        )
        return self._decode_multipart_message(response, stream=stream)

    def _http_get_application_pdf(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        stream: bool = False
    ) -> bytes:
        """GET a resource with "application/pdf" media type.

        Parameters
        ----------
        url: str
            Unique resource locator
        params: Union[Dict[str, Any], None], optional
            Additional HTTP GET query parameters
        rendered: bool, optional
            Whether resource should be requested using rendered media types
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        bytes
            Content of HTTP message body

        """
        response = self._http_get(
            url,
            params=params,
            headers={'Accept': 'application/pdf'},
            stream=stream
        )
        return response.content

    def _http_get_image(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        params: Optional[Dict[str, Any]] = None,
        stream: bool = False
    ) -> bytes:
        """GET a resource with "image/" media type.

        Parameters
        ----------
        url: str
            Unique resource locator
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Image media type (choices: ``"image/jpeg"``, ``"image/gif"``,
            ``"image/jp2"``, ``"image/png"``)
        params: Union[Dict[str, Any], None], optional
            Additional HTTP GET query parameters
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        bytes
            Content of HTTP message body

        """
        supported_media_types = {
            'image/',
            'image/*',
            'image/jpeg',
            'image/jp2',
            'image/gif',
            'image/png',
        }
        accept_header_field_value = self._build_accept_header_field_value(
            media_types,
            supported_media_types
        )
        response = self._http_get(
            url,
            params=params,
            headers={'Accept': accept_header_field_value},
            stream=stream
        )
        return response.content

    def _http_get_video(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        params: Optional[Dict[str, Any]] = None,
        stream: bool = False
    ) -> bytes:
        """GET a resource with "video/" media type.

        Parameters
        ----------
        url: str
            Unique resource locator
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Video media type (choices: ``"video/mpeg"``, ``"video/mp4"``,
            ``"video/H265"``)
        params: Union[Dict[str, Any], None], optional
            Additional HTTP GET query parameters
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        bytes
            Content of HTTP message body

        """
        supported_media_types = {
            'video/',
            'video/*',
            'video/mpeg',
            'video/mp4',
            'video/H265',
        }
        accept_header_field_value = self._build_accept_header_field_value(
            media_types,
            supported_media_types
        )
        response = self._http_get(
            url,
            params=params,
            headers={'Accept': accept_header_field_value},
            stream=stream
        )
        return response.content

    def _http_get_text(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        params: Optional[Dict[str, Any]] = None,
        stream: bool = False
    ) -> bytes:
        """GET a resource with "text/" media type.

        Parameters
        ----------
        url: str
            Unique resource locator
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Text media type (choices: ``"text/html"``, ``"text/plain"``,
            ``"text/xml"``, ``"text/rtf"``)
        params: Union[Dict[str, Any], None], optional
            Additional HTTP GET query parameters
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        bytes
            Content of HTTP message body

        """
        supported_media_types = {
            'text/',
            'text/*',
            'text/html',
            'text/plain',
            'text/rtf',
            'text/xml',
        }
        accept_header_field_value = self._build_accept_header_field_value(
            media_types, supported_media_types
        )
        response = self._http_get(
            url,
            params=params,
            headers={'Accept': accept_header_field_value},
            stream=stream
        )
        return response.content

    def _http_post(
        self,
        url: str,
        data: bytes,
        headers: Dict[str, str]
    ) -> requests.models.Response:
        """Perform a HTTP POST request.

        Parameters
        ----------
        url: str
            unique resource locator
        data: bytes
            HTTP request message payload
        headers: Dict[str, str]
            HTTP request message headers

        Returns
        -------
        requests.models.Response
            HTTP response message

        """
        def serve_data_chunks(data):
            for i, offset in enumerate(range(0, len(data), self._chunk_size)):
                logger.debug(f'serve data chunk #{i}')
                end = offset + self._chunk_size
                yield data[offset:end]

        @retrying.retry(
            retry_on_result=self._is_retriable_http_error,
            wait_exponential_multiplier=self._wait_exponential_multiplier,
            stop_max_attempt_number=self._max_attempts
        )
        def _invoke_post_request(
            url: str,
            data: bytes,
            headers: Optional[Dict[str, str]] = None
        ) -> requests.models.Response:
            logger.debug(f'POST: {url} {headers}')
            return self._session.post(url, data=data, headers=headers)

        if len(data) > self._chunk_size:
            logger.info('store data in chunks using chunked transfer encoding')
            chunked_headers = dict(headers)
            chunked_headers['Transfer-Encoding'] = 'chunked'
            chunked_headers['Cache-Control'] = 'no-cache'
            chunked_headers['Connection'] = 'Keep-Alive'
            data_chunks = serve_data_chunks(data)
            response = _invoke_post_request(url, data_chunks, chunked_headers)
        else:
            # There is a bug in the requests library that sets the Host header
            # again when using chunked transer encoding. Apparently this is
            # tricky to fix (see https://github.com/psf/requests/issues/4392).
            # As a temporary workaround we are only setting the header field,
            # if we don't use chunked transfer encoding.
            headers['Host'] = self.host
            response = _invoke_post_request(url, data, headers)
        logger.debug(f'request status code: {response.status_code}')
        response.raise_for_status()
        if not response.ok:
            logger.warning('storage was not successful for all instances')
            payload = response.content
            content_type = response.headers['Content-Type']
            if content_type in ('application/dicom+json', 'application/json', ):
                dataset = pydicom.dataset.Dataset.from_json(payload)
            elif content_type in ('application/dicom+xml', 'application/xml', ):
                tree = fromstring(payload)
                dataset = _load_xml_dataset(tree)
            else:
                raise ValueError('Response message has unexpected media type.')
            failed_sop_sequence = getattr(dataset, 'FailedSOPSequence', [])
            for failed_sop_item in failed_sop_sequence:
                logger.error(
                    'storage of instance {} failed: "{}"'.format(
                        failed_sop_item.ReferencedSOPInstanceUID,
                        failed_sop_item.FailureReason
                    )
                )
        return response

    def _http_post_multipart_application_dicom(
        self,
        url: str,
        data: Sequence[bytes]
    ) -> pydicom.Dataset:
        """POST a multipart resource with "application/dicom" media type.

        Parameters
        ----------
        url: str
            Unique resource locator
        data: Sequence[bytes]
            DICOM data sets that should be posted

        Returns
        -------
        pydicom.dataset.Dataset
            Information about stored instances

        """
        content_type = (
            'multipart/related; '
            'type="application/dicom"; '
            'boundary="0f3cf5c0-70e0-41ef-baef-c6f9f65ec3e1"'
        )
        content = self._encode_multipart_message(data, content_type)
        response = self._http_post(
            url,
            content,
            headers={'Content-Type': content_type}
        )
        if response.content:
            content_type = response.headers['Content-Type']
            if content_type in ('application/dicom+json', 'application/json', ):
                return pydicom.Dataset.from_json(response.json())
            elif content_type in ('application/dicom+xml', 'application/xml', ):
                tree = fromstring(response.content)
                return _load_xml_dataset(tree)
        return pydicom.Dataset()

    def _http_delete(self, url: str):
        """Perform a HTTP DELETE request.

        Parameters
        ----------
        url: str
            Unique resource locator

        Returns
        -------
        requests.models.Response
            HTTP response message

        """
        @retrying.retry(
            retry_on_result=self._is_retriable_http_error,
            wait_exponential_multiplier=self._wait_exponential_multiplier,
            stop_max_attempt_number=self._max_attempts
        )
        def _invoke_delete_request(url: str) -> requests.models.Response:
            return self._session.delete(url)

        response = _invoke_delete_request(url)
        if response.status_code == HTTPStatus.METHOD_NOT_ALLOWED:
            logger.error(
              'Resource could not be deleted. The origin server may not support'
              'deletion or you may not have the necessary permissions.')
        response.raise_for_status()
        return response

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
        logger.info('search for studies')
        url = self._get_studies_url(_Transaction.SEARCH)
        params = self._parse_qido_query_parameters(
            fuzzymatching, limit, offset, fields, search_filters
        )
        return self._http_get_application_json(url, params)

    @classmethod
    def _parse_media_type(cls, media_type: str) -> Tuple[str, str]:
        """Parse media type and extract its type and subtype.

        Parameters
        ----------
        media_type: str
            Media type, e.g., ``"image/jpeg"``

        Returns
        -------
        Tuple[str, str]
            Type and subtype of media type (``("image", "jpeg")``)

        Raises
        ------
        ValueError
            When `media_type` is invalid

        """
        cls._assert_media_type_is_valid(media_type)
        media_type_type, media_type_subtype = media_type.split('/')
        return media_type_type, media_type_subtype

    @classmethod
    def _get_common_media_type(
        cls,
        media_types: Tuple[Union[str, Tuple[str, str]]]
    ) -> str:
        """Get common type of acceptable media types.

        Also asserts that only one type of a given category of DICOM resources
        (``"application/dicom"``), compressed bulkdata resources
        (``"image/"``, ``"video/"``) or uncompressed bulkdata resources
        (``"application/octet-stream"``).
        For example, ``("image/jpeg", "image/jp2")`` or
        ``("application/dicom", "application/dicom")`` will pass and
        return ``"image/"`` or ``"application/dicom"``, respectively. However,
        ``("image/jpeg", "video/mpeg2")`` or
        ``("application/dicom", "application/octet-stream")``
        will raise an exception.

        Parameters
        ----------
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        str
            Common type of media type category

        Raises
        ------
        ValueError
            when no media types are provided or more than one type is provided

        """
        if media_types is None:
            raise ValueError('No acceptable media types provided.')
        common_media_types = []
        for item in media_types:
            if isinstance(item, str):
                media_type = item
            else:
                media_type = item[0]
            if media_type.startswith('application'):
                common_media_types.append(media_type)
            else:
                mtype, msubtype = cls._parse_media_type(media_type)
                common_media_types.append(f'{mtype}/')
        if len(set(common_media_types)) == 0:
            raise ValueError(
                'No common acceptable media type could be identified.'
            )
        elif len(set(common_media_types)) > 1:
            raise ValueError('Acceptable media types must have the same type.')
        return common_media_types[0]

    def _get_bulkdata(
        self,
        url: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        byte_range: Optional[Tuple[int, int]] = None,
        stream: bool = False,
    ) -> Iterator[bytes]:
        """Get bulk data items at a given location.

        Parameters
        ----------
        url: str
            Location of the bulk data
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        byte_range: Union[Tuple[int, int], None], optional
            Start and end of byte range
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        Iterator[bytes]
            Bulk data items

        """
        if media_types is None:
            return self._http_get_multipart_application_octet_stream(
                url, media_types, byte_range=byte_range, stream=stream
            )
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type == 'application/octet-stream':
            return self._http_get_multipart_application_octet_stream(
                url, media_types, byte_range=byte_range, stream=stream
            )
        elif common_media_type.startswith('image'):
            return self._http_get_multipart_image(
                url, media_types, byte_range=byte_range, stream=stream
            )
        elif common_media_type.startswith('video'):
            return self._http_get_multipart_video(
                url, media_types, byte_range=byte_range, stream=stream
            )
        else:
            raise ValueError(
                f'Media type "{common_media_type}" is not supported for '
                'retrieval of bulkdata.'
            )

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
        Iterator[bytes]
            Bulk data items

        """
        return list(
            self._get_bulkdata(
                url=url,
                media_types=media_types,
                byte_range=byte_range,
                stream=False
            )
        )

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

        Note
        ----
        Data is streamed from the DICOMweb server.

        """
        return self._get_bulkdata(
            url=url,
            media_types=media_types,
            byte_range=byte_range,
            stream=True
        )

    def _get_study(
        self,
        study_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        stream: bool = False
    ) -> Iterator[pydicom.dataset.Dataset]:
        """Get all instances of a study.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxescceptable transfer syntax UIDs
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        Iterator[pydicom.dataset.Dataset]
            Instances

        """
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of study.'
            )
        url = self._get_studies_url(_Transaction.RETRIEVE, study_instance_uid)
        if media_types is None:
            return self._http_get_multipart_application_dicom(
                url,
                stream=stream
            )
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type != 'application/dicom':
            raise ValueError(
                f'Media type "{common_media_type}" is not supported for '
                'retrieval of a study. It must be "application/dicom".'
            )
        return self._http_get_multipart_application_dicom(
            url,
            media_types=media_types,
            stream=stream
        )

    def retrieve_study(
        self,
        study_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
    ) -> List[pydicom.dataset.Dataset]:
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
        return list(
            self._get_study(
                study_instance_uid=study_instance_uid,
                media_types=media_types,
                stream=False
            )
        )

    def iter_study(
        self,
        study_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
    ) -> Iterator[pydicom.dataset.Dataset]:
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

        Note
        ----
        Data is streamed from the DICOMweb server.

        """
        return self._get_study(
            study_instance_uid=study_instance_uid,
            media_types=media_types,
            stream=True
        )

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
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of '
                'study metadata.'
            )
        url = self._get_studies_url(_Transaction.RETRIEVE, study_instance_uid)
        url += '/metadata'
        return self._http_get_application_json(url)

    def delete_study(self, study_instance_uid: str) -> None:
        """Delete all instances of a study.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID

        Note
        ----
        The Delete Study resource is not part of the DICOM standard
        and may not be supported by all origin servers.

        Warning
        -------
        This method performs a DELETE and should be used with caution.

        """
        if study_instance_uid is None:
            raise ValueError(
              'Study Instance UID is required for deletion of a study.'
            )
        url = self._get_studies_url(_Transaction.DELETE, study_instance_uid)
        self._http_delete(url)

    def _assert_uid_format(self, uid: str) -> None:
        """Check whether a DICOM UID has the correct format.

        Parameters
        ----------
        uid: str
            DICOM UID

        Raises
        ------
        TypeError
            When `uid` is not a string
        ValueError
            When `uid` doesn't match the regular expression pattern
            ``"^[.0-9]+$"``

        """
        if not isinstance(uid, str):
            raise TypeError('DICOM UID must be a string.')
        pattern = re.compile('^[.0-9]+$')
        if not pattern.search(uid):
            raise ValueError('DICOM UID has invalid format.')

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
        if study_instance_uid is not None:
            self._assert_uid_format(study_instance_uid)
            logger.info(f'search for series of study "{study_instance_uid}"')
        else:
            logger.info('search for series')
        url = self._get_series_url(_Transaction.SEARCH, study_instance_uid)
        params = self._parse_qido_query_parameters(
            fuzzymatching, limit, offset, fields, search_filters
        )
        return self._http_get_application_json(url, params)

    def _get_series(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        stream: bool = False
    ) -> Iterator[pydicom.dataset.Dataset]:
        """Get instances of a series.

        Parameters
        ----------
        study_instance_uid: str
            Study Instance UID
        series_instance_uid: str
            Series Instance UID
        media_types: Union[Tuple[Union[str, Tuple[str, str]]], None], optional
            Acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        Iterator[pydicom.dataset.Dataset]
            Instances

        """
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of series.'
            )
        logger.info(
            f'retrieve series "{series_instance_uid}" '
            f'of study "{study_instance_uid}"'
        )
        self._assert_uid_format(study_instance_uid)
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of series.'
            )
        self._assert_uid_format(series_instance_uid)
        url = self._get_series_url(
            _Transaction.RETRIEVE,
            study_instance_uid,
            series_instance_uid
        )
        if media_types is None:
            return self._http_get_multipart_application_dicom(
                url,
                stream=stream
            )
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type != 'application/dicom':
            raise ValueError(
                f'Media type "{common_media_type}" is not supported for '
                'retrieval of a series. It must be "application/dicom".'
            )
        return self._http_get_multipart_application_dicom(
            url,
            media_types=media_types,
            stream=stream
        )

    def retrieve_series(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None
    ) -> List[pydicom.dataset.Dataset]:
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
        return list(
            self._get_series(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                media_types=media_types,
                stream=False
            )
        )

    def iter_series(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None
    ) -> Iterator[pydicom.dataset.Dataset]:
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

        Note
        ----
        Data is streamed from the DICOMweb server.

        """
        return self._get_series(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            media_types=media_types,
            stream=True
        )

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
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of '
                'series metadata.'
            )
        self._assert_uid_format(study_instance_uid)
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of '
                'series metadata.'
            )
        logger.info(
            f'retrieve metadata of series "{series_instance_uid}" '
            f'of study "{study_instance_uid}"'
        )
        self._assert_uid_format(series_instance_uid)
        url = self._get_series_url(
            _Transaction.RETRIEVE,
            study_instance_uid,
            series_instance_uid
        )
        url += '/metadata'
        return self._http_get_application_json(url)

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
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of '
                'rendered series.'
            )
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of '
                'rendered series.'
            )
        logger.info(
            f'retrieve rendered series "{series_instance_uid}" '
            f'of study "{study_instance_uid}"'
        )
        url = self._get_series_url(
            _Transaction.RETRIEVE,
            study_instance_uid,
            series_instance_uid
        )
        url += '/rendered'
        if media_types is None:
            response = self._http_get(url, params)
            return response.content
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type.startswith('image'):
            return self._http_get_image(url, media_types, params)
        elif common_media_type.startswith('video'):
            return self._http_get_video(url, media_types, params)
        elif common_media_type.startswith('text'):
            return self._http_get_text(url, media_types, params)
        elif common_media_type == 'application/pdf':
            return self._http_get_application_pdf(url, params)
        else:
            raise ValueError(
                f'Media type "{common_media_type}" is not supported for '
                'retrieval of rendered series.'
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

        Note
        ----
        The Delete Series resource is not part of the DICOM standard
        and may not be supported by all origin servers.

        Warning
        -------
        This method performs a DELETE and should be used with caution.

        """
        if study_instance_uid is None:
            raise ValueError(
              'Study Instance UID is required for deletion of a series.'
            )
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for deletion of a series.'
            )
        logger.info(
            f'delete series "{series_instance_uid}" '
            f'of study "{study_instance_uid}"'
        )
        url = self._get_series_url(
            _Transaction.DELETE,
            study_instance_uid,
            series_instance_uid
        )
        self._http_delete(url)

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
        message = 'search for instances'
        if series_instance_uid is not None:
            message += f' of series "{series_instance_uid}"'
        if study_instance_uid is not None:
            self._assert_uid_format(study_instance_uid)
            message += f' of study "{study_instance_uid}"'
        logger.info(message)
        url = self._get_instances_url(
            _Transaction.SEARCH,
            study_instance_uid,
            series_instance_uid
        )
        params = self._parse_qido_query_parameters(
            fuzzymatching, limit, offset, fields, search_filters
        )
        return self._http_get_application_json(url, params)

    def retrieve_instance(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
    ) -> pydicom.dataset.Dataset:
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
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of instance.'
            )
        self._assert_uid_format(study_instance_uid)
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of instance.'
            )
        self._assert_uid_format(series_instance_uid)
        if sop_instance_uid is None:
            raise ValueError(
                'SOP Instance UID is required for retrieval of instance.'
            )
        logger.info(
            f'retrieve instance "{sop_instance_uid}" '
            f'of series "{series_instance_uid}" '
            f'of study "{study_instance_uid}"'
        )
        self._assert_uid_format(sop_instance_uid)
        url = self._get_instances_url(
            _Transaction.RETRIEVE,
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid
        )
        if media_types is not None:
            common_media_type = self._get_common_media_type(media_types)
            if common_media_type != 'application/dicom':
                raise ValueError(
                    f'Media type "{common_media_type}" is not supported for '
                    'retrieval of an instance. It must be "application/dicom".'
                )
        iterator = self._http_get_multipart_application_dicom(url, media_types)
        instances = list(iterator)
        if len(instances) > 1:
            # This should not occur, but safety first.
            raise ValueError('More than one instance returned by origin server')
        return instances[0]

    def store_instances(
        self,
        datasets: Sequence[pydicom.dataset.Dataset],
        study_instance_uid: Optional[str] = None
    ) -> pydicom.dataset.Dataset:
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
        url = self._get_studies_url(_Transaction.STORE, study_instance_uid)
        encoded_datasets = list()
        for ds in datasets:
            with BytesIO() as b:
                pydicom.dcmwrite(b, ds)
                encoded_ds = b.getvalue()
            encoded_datasets.append(encoded_ds)
        return self._http_post_multipart_application_dicom(
            url,
            encoded_datasets
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

        Note
        ----
        The Delete Instance resource is not part of the DICOM standard
        and may not be supported by all origin servers.

        Warning
        -------
        This method performs a DELETE and should be used with caution.

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
            _Transaction.DELETE,
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid
        )
        self._http_delete(url)

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
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of '
                'instance metadata.'
            )
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of '
                'instance metadata.'
            )
        if sop_instance_uid is None:
            raise ValueError(
                'SOP Instance UID is required for retrieval of '
                'instance metadata.'
            )
        url = self._get_instances_url(
            _Transaction.RETRIEVE,
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid
        )
        url += '/metadata'
        return self._http_get_application_json(url)[0]

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
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of '
                'rendered instance.'
            )
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of '
                'rendered instance.'
            )
        if sop_instance_uid is None:
            raise ValueError(
                'SOP Instance UID is required for retrieval of '
                'rendered instance.'
            )
        url = self._get_instances_url(
            _Transaction.RETRIEVE,
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid
        )
        url += '/rendered'
        if media_types is None:
            response = self._http_get(url, params)
            return response.content
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type.startswith('image'):
            return self._http_get_image(url, media_types, params)
        elif common_media_type.startswith('video'):
            return self._http_get_video(url, media_types, params)
        elif common_media_type.startswith('text'):
            return self._http_get_text(url, media_types, params)
        elif common_media_type == 'application/pdf':
            return self._http_get_application_pdf(url, params)
        else:
            raise ValueError(
                f'Media type "{common_media_type}" is not supported for '
                'retrieval of rendered instance.'
            )

    def _get_instance_frames(
        self,
        study_instance_uid: str,
        series_instance_uid: str,
        sop_instance_uid: str,
        frame_numbers: Sequence[int],
        media_types: Optional[Tuple[Union[str, Tuple[str, str]]]] = None,
        stream: bool = False
    ) -> Iterator[bytes]:
        """Get frames of an instance.

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
        stream: bool, optional
            Whether data should be streamed (i.e., requested using chunked
            transfer encoding)

        Returns
        -------
        Iterator[bytes]
            Pixel data for each frame

        """
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of frames.'
            )
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of frames.'
            )
        if sop_instance_uid is None:
            raise ValueError(
                'SOP Instance UID is required for retrieval of frames.'
            )
        url = self._get_instances_url(
            _Transaction.RETRIEVE,
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid
        )
        frame_list = ','.join([str(n) for n in frame_numbers])
        url += f'/frames/{frame_list}'
        if media_types is None:
            return self._http_get_multipart_application_octet_stream(
                url,
                stream=stream
            )
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type == 'application/octet-stream':
            return self._http_get_multipart_application_octet_stream(
                url,
                media_types=media_types,
                stream=stream
            )
        elif common_media_type.startswith('image'):
            return self._http_get_multipart_image(
                url,
                media_types=media_types,
                stream=stream
            )
        elif common_media_type.startswith('video'):
            return self._http_get_multipart_video(
                url,
                media_types=media_types,
                stream=stream
            )
        else:
            raise ValueError(
                f'Media type "{common_media_type}" is not supported for '
                'retrieval of frames.'
            )

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
        return list(
            self._get_instance_frames(
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                sop_instance_uid=sop_instance_uid,
                frame_numbers=frame_numbers,
                media_types=media_types,
                stream=False
            )
        )

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

        Note
        ----
        Data is streamed from the DICOMweb server.

        """
        return self._get_instance_frames(
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            sop_instance_uid=sop_instance_uid,
            frame_numbers=frame_numbers,
            media_types=media_types,
            stream=True
        )

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
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of '
                'rendered frame.'
            )
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of '
                'rendered frame.'
            )
        if sop_instance_uid is None:
            raise ValueError(
                'SOP Instance UID is required for retrieval of rendered frame.'
            )
        url = self._get_instances_url(
            _Transaction.RETRIEVE,
            study_instance_uid,
            series_instance_uid,
            sop_instance_uid
        )
        url += '/frames/{frame_numbers}/rendered'.format(
            frame_numbers=','.join([str(n) for n in frame_numbers])
        )
        if media_types is None:
            # Try and hope for the best...
            response = self._http_get(url, params)
            return response.content
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type.startswith('image'):
            return self._http_get_image(url, media_types, params)
        elif common_media_type.startswith('video'):
            return self._http_get_video(url, media_types, params)
        else:
            raise ValueError(
                f'Media type "{common_media_type}" is not supported for '
                'retrieval of rendered frame.'
            )

    @staticmethod
    def lookup_keyword(
        tag: Union[str, int, Tuple[str, str], pydicom.tag.BaseTag]
    ) -> str:
        """Look up the keyword of a DICOM attribute.

        Parameters
        ----------
        tag: Union[str, int, Tuple[str, str], pydicom.tag.BaseTag]
            Attribute tag (e.g. ``"00080018"``)

        Returns
        -------
        str
            Attribute keyword (e.g. ``"SOPInstanceUID"``)

        """
        return pydicom.datadict.keyword_for_tag(tag)

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
        tag = pydicom.datadict.tag_for_keyword(keyword)
        tag = pydicom.tag.Tag(tag)
        return '{0:04x}{1:04x}'.format(tag.group, tag.element).upper()
