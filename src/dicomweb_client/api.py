'''Application Programming Interface (API).'''
import re
import os
import sys
import logging
import email
import six
from io import BytesIO
from collections import OrderedDict
if sys.version_info.major < 3:
    from urllib import quote_plus
    from urlparse import urlparse
else:
    from urllib.parse import quote_plus, urlparse

import requests
import pydicom

from dicomweb_client.error import DICOMJSONError, HTTPError


logger = logging.getLogger(__name__)


def _init_dataset():
    '''Creates an empty DICOM Data Set.

    Returns
    -------
    pydicom.dataset.Dataset

    '''
    return pydicom.dataset.Dataset()


def _create_dataelement(tag, vr, value):
    '''Creates a DICOM Data Element.

    Parameters
    ----------
    tag: pydicom.tag.Tag
        data element tag
    vr: str
        data element value representation
    value: list
        data element value(s)

    Returns
    -------
    pydicom.dataelem.DataElement

    '''
    binary_representations = {'OB', 'OD', 'OF', 'OL', 'OW', 'UN'}
    try:
        vm = pydicom.datadict.dictionary_VM(tag)
    except KeyError:
        # Private tag
        vm = str(len(value))
    if vr not in binary_representations:
        if not(isinstance(value, list)):
            raise DICOMJSONError(
                '"Value" of data element "{}" must be an array.'.format(tag)
            )
    if vr == 'SQ':
        elem_value = []
        for value_item in value:
            ds = _init_dataset()
            if value_item is not None:
                for key, val in value_item.items():
                    if 'vr' not in val:
                        raise DICOMJSONError(
                            'Data element "{}" must have key "vr".'.format(tag)
                        )
                    supported_keys = {'Value', 'BulkDataURI', 'InlineBinary'}
                    val_key = None
                    for k in supported_keys:
                        if k in val:
                            val_key = k
                            break
                    if val_key is None:
                        logger.debug(
                            'data element has neither key "{}".'.format(
                                '" nor "'.join(supported_keys)
                            )
                        )
                        e = pydicom.dataelem.DataElement(
                            tag=tag, value=None, VR=vr
                        )
                    else:
                        e = _create_dataelement(key, val['vr'], val[val_key])
                    ds.add(e)
            elem_value.append(ds)
    elif vr == 'PN':
        # Special case, see DICOM Part 18 Annex F2.2
        elem_value = []
        for v in value:
            if not isinstance(v, dict):
                # Some DICOMweb services get this wrong, so we workaround the
                # the issue and warn the user rather than raising an error.
                logger.warning(
                    'attribute with VR Person Name (PN) is not '
                    'formatted correctly'
                )
                elem_value.append(v)
            else:
                elem_value.extend(list(v.values()))
        if vm == '1':
            try:
                elem_value = elem_value[0]
            except IndexError:
                elem_value = None
    else:
        if vm == '1':
            if vr in binary_representations:
                elem_value = value
            else:
                if value:
                    elem_value = value[0]
                else:
                    elem_value = value
        else:
            if len(value) == 1 and isinstance(value[0], six.string_types):
                elem_value = value[0].split('\\')
            else:
                elem_value = value
    if value is None:
        logger.warning('missing value for data element "{}"'.format(tag))
    try:
        return pydicom.dataelem.DataElement(tag=tag, value=elem_value, VR=vr)
    except Exception:
        raise ValueError(
            'Data element "{}" could not be loaded from JSON: {}'.format(
                tag, elem_value
            )
        )


def load_json_dataset(dataset):
    '''Loads DICOM Data Set in DICOM JSON format.

    Parameters
    ----------
    dataset: Dict[str, dict]
        mapping where keys are DICOM *Tags* and values are mappings of DICOM
        *VR* and *Value* key-value pairs

    Returns
    -------
    pydicom.dataset.Dataset

    '''
    ds = _init_dataset()
    for tag, mapping in dataset.items():
        vr = mapping['vr']
        try:
            value = mapping['Value']
        except KeyError:
            logger.debug(
                'mapping for data element "{}" has no "Value" key'.format(tag)
            )
            value = [None]
        de = _create_dataelement(tag, vr, value)
        ds.add(de)
    return ds


class DICOMwebClient(object):

    '''Class for connecting to and interacting with a DICOMweb RESTful service.

    Attributes
    ----------
    base_url: str
        unique resource locator of the DICOMweb service
    protocol: str
        name of the protocol, e.g. ``"https"``
    host: str
        IP address or DNS name of the machine that hosts the server
    port: int
        number of the port to which the server listens
    url_prefix: str
        URL path prefix for DICOMweb services (part of `base_url`)
    qido_url_prefix: Union[str, NoneType]
        URL path prefix for QIDO-RS (not part of `base_url`)
    wado_url_prefix: Union[str, NoneType]
        URL path prefix for WADO-RS (not part of `base_url`)
    stow_url_prefix: Union[str, NoneType]
        URL path prefix for STOW-RS (not part of `base_url`)

    '''

    def __init__(self, url, username=None, password=None, ca_bundle=None,
                 cert=None, qido_url_prefix=None, wado_url_prefix=None,
                 stow_url_prefix=None, proxies=None, headers=None,
                 callback=None, auth=None):
        '''
        Parameters
        ----------
        url: str
            base unique resource locator consisting of protocol, hostname
            (IP address or DNS name) of the machine that hosts the server and
            optionally port number and path prefix
        username: str, optional
            username for authentication with services
        password: str, optional
            password for authentication with services
        ca_bundle: str, optional
            path to CA bundle file
        cert: str, optional
            path to client certificate file in Privacy Enhanced Mail (PEM)
            format
        qido_url_prefix: str, optional
            URL path prefix for QIDO RESTful services
        wado_url_prefix: str, optional
            URL path prefix for WADO RESTful services
        stow_url_prefix: str, optional
            URL path prefix for STOW RESTful services
        proxies: dict, optional
            mapping of protocol or protocol + host to the URL of a proxy server
        headers: dict, optional
            custom headers that should be included in request messages,
            e.g., authentication tokens
        callback: Callable, optional
            callback function to manipulate responses generated from requests
            (see `requests event hooks <http://docs.python-requests.org/en/master/user/advanced/#event-hooks>`_)
        auth: requests.auth.AuthBase, optional
            a subclassed instance of `requests.auth.AuthBase` to be used for authentication with the DICOMweb server

        '''  # noqa
        logger.debug('initialize HTTP session')
        self._session = requests.Session()
        self.base_url = url
        self.qido_url_prefix = qido_url_prefix
        self.wado_url_prefix = wado_url_prefix
        self.stow_url_prefix = stow_url_prefix
        if self.base_url.startswith('https'):
            if ca_bundle is not None:
                ca_bundle = os.path.expanduser(os.path.expandvars(ca_bundle))
                if not os.path.exists(ca_bundle):
                    raise OSError(
                        'CA bundle file does not exist: {}'.format(ca_bundle)
                    )
                logger.debug('use CA bundle file: {}'.format(ca_bundle))
                self._session.verify = ca_bundle
            if cert is not None:
                cert = os.path.expanduser(os.path.expandvars(cert))
                if not os.path.exists(cert):
                    raise OSError(
                        'Certificate file does not exist: {}'.format(cert)
                    )
                logger.debug('use certificate file: {}'.format(cert))
                self._session.cert = cert

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
        try:
            self.protocol = match.group('scheme')
            self.host = match.group('host')
            self.port = match.group('port')
        except AttributeError:
            raise ValueError('Malformed URL: {}'.format(self.base_url))
        if self.port is not None:
            self.port = int(self.port)
        else:
            if self.protocol == 'http':
                self.port = 80
            elif self.protocol == 'https':
                self.port = 443
            else:
                raise ValueError(
                    'URL scheme "{}" is not supported.'.format(self.protocol)
                )
        url_components = urlparse(url)
        self.url_prefix = url_components.path
        self._session.headers.update({'Host': self.host})
        if headers is not None:
            self._session.headers.update(headers)
        self._session.proxies = proxies
        if callback is not None:
            self._session.hooks = {'response': callback}
        if auth is not None:
            self._session.auth = auth
            if username or password:
                logger.warning(
                    'Auth object specified. '
                    'Username and password ignored.'
                )
        if username is not None:
            if not password:
                raise ValueError(
                    'No password provided for user "{0}".'.format(username)
                )
            self._session.auth = (username, password)

    def _parse_qido_query_parameters(self, fuzzymatching, limit, offset,
                                     fields, search_filters):
        '''Parses query parameters for inclusion into a HTTP query string
        of a QIDO-RS request message.

        Parameters
        ----------
        fuzzymatching: bool, optional
            whether fuzzy semantic matching should be performed
        limit: int, optional
            maximum number of results that should be returned
        offset: int, optional
            number of results that should be skipped
        fields: List[str], optional
            names of fields (attributes) that should be included in results
        search_filters: Dict[str, Any], optional
            search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match

        Returns
        -------
        collections.OrderedDict
            sanitized and sorted query parameters

        '''
        params = {}
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
            params['includefield'] = []
            for field in set(fields):
                if not(isinstance(field, str)):
                    raise TypeError('Elements of "fields" must be a string.')
                params['includefield'].append(field)
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

    def _get_service_url(self, service):
        '''Constructes the URL of a DICOMweb RESTful services.

        Parameters
        ----------
        service: str
            name of the RESTful service
            (choices: ``"quido"``, ``"wado"``, or ``"stow"``)

        Returns
        -------
        str
            full URL for the given service

        '''
        service_url = self.base_url
        if service == 'qido':
            if self.qido_url_prefix is not None:
                service_url += '/{}'.format(self.qido_url_prefix)
        elif service == 'wado':
            if self.wado_url_prefix is not None:
                service_url += '/{}'.format(self.wado_url_prefix)
        elif service == 'stow':
            if self.stow_url_prefix is not None:
                service_url += '/{}'.format(self.stow_url_prefix)
        else:
            raise ValueError(
                'Unsupported DICOMweb service "{}".'.format(service)
            )
        return service_url

    def _get_studies_url(self, service, study_instance_uid=None):
        '''Constructes the URL for study-level requests.

        Parameters
        ----------
        service: str
            name of the RESTful service
            (choices: ``"quido"``, ``"wado"``, or ``"stow"``)
        study_instance_uid: str, optional
            unique study identifier

        Returns
        -------
        str
            URL

        '''
        if study_instance_uid is not None:
            url = '{service_url}/studies/{study_instance_uid}'
        else:
            url = '{service_url}/studies'
        service_url = self._get_service_url(service)
        return url.format(
            service_url=service_url, study_instance_uid=study_instance_uid
        )

    def _get_series_url(self, service, study_instance_uid=None,
                        series_instance_uid=None):
        '''Constructes the URL for series-level requests.

        Parameters
        ----------
        service: str
            name of the RESTful service
            (choices: ``"quido"``, ``"wado"``, or ``"stow"``)
        study_instance_uid: str, optional
            unique study identifier
        series_instance_uid: str, optional
            unique series identifier

        Returns
        -------
        str
            URL

        '''
        if study_instance_uid is not None:
            url = self._get_studies_url(service, study_instance_uid)
            if series_instance_uid is not None:
                url += '/series/{series_instance_uid}'
            else:
                url += '/series'
        else:
            if series_instance_uid is not None:
                logger.warning(
                    'series UID is ignored because study UID is undefined'
                )
            url = '{service_url}/series'
        service_url = self._get_service_url(service)
        return url.format(
            service_url=service_url, series_instance_uid=series_instance_uid
        )

    def _get_instances_url(self, service, study_instance_uid=None,
                           series_instance_uid=None, sop_instance_uid=None):
        '''Constructes the URL for instance-level requests.

        Parameters
        ----------
        service: str
            name of the RESTful service
            (choices: ``"quido"``, ``"wado"``, or ``"stow"``)
        study_instance_uid: str, optional
            unique study identifier
        series_instance_uid: str, optional
            unique series identifier
        sop_instance_uid: str, optional
            unique instance identifier

        Returns
        -------
        str
            URL

        '''
        if study_instance_uid is not None and series_instance_uid is not None:
            url = self._get_series_url(
                service, study_instance_uid, series_instance_uid
            )
            url += '/instances'
            if sop_instance_uid is not None:
                url += '/{sop_instance_uid}'
        else:
            if sop_instance_uid is not None:
                logger.warning(
                    'SOP Instance UID is ignored because Study/Series '
                    'Instance UID are undefined'
                )
            url = '{service_url}/instances'
        service_url = self._get_service_url(service)
        return url.format(
            service_url=service_url, sop_instance_uid=sop_instance_uid
        )

    def _build_query_string(self, params):
        '''Builds a HTTP query string for a GET request message.

        Parameters
        ----------
        params: dict
            query parameters as mapping of key-value pairs;
            in case a key should be included more than once with different
            values, values need to be provided in form of an iterable (e.g.,
            ``{"key": ["value1", "value2"]}`` will result in
            ``"?key=value1&key=value2"``)

        Returns
        -------
        str
            query string

        '''
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

    def _http_get(self, url, params=None, headers=None):
        '''Performs a HTTP GET request.

        Parameters
        ----------
        url: str
            unique resource locator
        params: Dict[str], optional
            query parameters
        headers: Dict[str], optional
            HTTP request message headers

        Returns
        -------
        requests.models.Response
            HTTP response message

        '''
        if headers is None:
            headers = {}
        if params is None:
            params = {}
        url += self._build_query_string(params)
        logger.debug('GET: {} {}'.format(url, headers))
        response = self._session.get(url=url, headers=headers)
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as error:
            raise HTTPError(error)
        logger.debug('request status code: {}'.format(response.status_code))
        if response.status_code == 204:
            logger.warning('empty response')
        # The server may not return all results, but rather include a warning
        # header to notify that client that there are remaining results.
        # (see DICOM Part 3.18 Section 6.7.1.2)
        if 'Warning' in response.headers:
            logger.warning(response.headers['Warning'])
        return response

    def _http_get_application_json(self, url, params=None):
        '''Performs a HTTP GET request that accepts "applicaton/dicom+json"
        media type.

        Parameters
        ----------
        url: str
            unique resource locator
        params: Dict[str], optional
            query parameters

        Returns
        -------
        Union[dict, list, types.NoneType]
            content of HTTP message body in DICOM JSON format

        '''
        content_type = 'application/dicom+json'
        resp = self._http_get(url, params, {'Accept': content_type})
        if resp.content:
            return resp.json()

    def _decode_multipart_message(self, body, headers):
        '''Extracts parts of a HTTP multipart response message.

        Parameters
        ----------
        body: Union[str, bytes]
            HTTP response message body
        headers: Dict[str, str]
            HTTP response message headers

        Returns
        -------
        List[Union[str, bytes]]
            data

        '''
        header = ''
        for key, value in headers.items():
            header += '{}: {}\n'.format(key, value)
        if isinstance(body, str):
            message = email.message_from_string(header + body)
        else:
            message = email.message_from_bytes(header.encode() + body)
        elements = list()
        for part in message.walk():
            if part.get_content_maintype() == 'multipart':
                # Some servers don't handle this correctly.
                # If only one frame number is provided, return a normal
                # message body instead of a multipart message body.
                if part.is_multipart():
                    continue
            payload = part.get_payload(decode=True)
            elements.append(payload)
        return elements

    def _encode_multipart_message(self, data, content_type):
        '''Encodes the payload of a HTTP multipart response message.

        Parameters
        ----------
        data: List[Union[str, bytes]]
            data
        content_type: str
            content type of the multipart HTTP request message

        Returns
        -------
        Union[str, bytes]
            HTTP request message body

        '''
        multipart, content_type, boundary = content_type.split(';')
        boundary = boundary[boundary.find('=') + 2:-1]
        content_type = content_type[content_type.find('=') + 2:-1]
        body = b''
        for payload in data:
            body += (
                '\r\n--{boundary}'
                '\r\nContent-Type: {content_type}\r\n\r\n'.format(
                    boundary=boundary, content_type=content_type
                ).encode('utf-8')
            )
            body += payload
        body += '\r\n--{boundary}--'.format(boundary=boundary).encode('utf-8')
        return body

    def _assert_media_type_is_valid(self, media_type):
        '''Asserts that a given media type is valid.

        Parameters
        ----------
        media_type: str
            media type

        Raises
        ------
        ValueError
            when `media_type` is invalid

        '''
        error_message = 'Not a valid media type: "{}"'.format(media_type)
        sep_index = media_type.find('/')
        if sep_index == -1:
            raise ValueError(error_message)
        media_type_type = media_type[:sep_index]
        if media_type_type not in {'application', 'image', 'text', 'video'}:
            raise ValueError(error_message)
        if media_type.find('/', sep_index + 1) > 0:
            raise ValueError(error_message)

    def _build_range_header_field_value(self, byte_range):
        '''Builds a range header field value for HTTP GET request messages.

        Parameters
        ----------
        byte_range: Tuple[int], optional
            start and end of byte range

        Returns
        -------
        str
            range header field value

        '''
        if byte_range is not None:
            start = str(byte_range[0])
            try:
                end = byte_range[1]
            except IndexError:
                end = ''
            range_header_field_value = 'bytes={}-{}'.format(start, end)
        else:
            range_header_field_value = 'bytes=0-'
        return range_header_field_value

    def _build_accept_header_field_value(self, media_types,
                                         supported_media_types):
        '''Builds an accept header field value for HTTP GET request messages.

        Parameters
        ----------
        media_types: Tuple[str]
            acceptable media types
        supported_media_types: Set[str]
            supported media types

        Returns
        -------
        str
            accept header field value

        '''
        if not isinstance(media_types, (list, tuple, set)):
            raise TypeError(
                'Acceptable media types must be provided as a tuple.'
            )
        field_value_parts = []
        for media_type in media_types:
            if not isinstance(media_type, six.string_types):
                raise TypeError(
                    'Media type "{}" is not supported for '
                    'requested resource'.format(media_type)
                )
            self._assert_media_type_is_valid(media_type)
            if media_type not in supported_media_types:
                raise ValueError(
                    'Media type "{}" is not supported for '
                    'requested resource'.format(media_type)
                )
            field_value_parts.append(media_type)
        return ', '.join(field_value_parts)

    def _build_multipart_accept_header_field_value(self,
                                                   media_types,
                                                   supported_media_types):
        '''Builds an accept header field value for HTTP GET multipart request
        messages.

        Parameters
        ----------
        media_types: Tuple[Union[str, Tuple[str]]]
            acceptable media types and optionally the UIDs of the corresponding
            transfer syntaxes
        supported_media_types: Union[Dict[str, str], Set[str]]
            set of supported media types or mapping of transfer syntaxes
            to their corresponding media types

        Returns
        -------
        str
            accept header field value

        '''
        if not isinstance(media_types, (list, tuple, set)):
            raise TypeError(
                'Acceptable media types must be provided as a tuple.'
            )
        field_value_parts = []
        for item in media_types:
            if isinstance(item, six.string_types):
                media_type = item
                transfer_syntax_uid = None
            else:
                media_type = item[0]
                try:
                    transfer_syntax_uid = item[1]
                except IndexError:
                    transfer_syntax_uid = None
            self._assert_media_type_is_valid(media_type)
            field_value = 'multipart/related; type="{}"'.format(media_type)
            if isinstance(supported_media_types, dict):
                if media_type not in supported_media_types.values():
                    if (not media_type.endswith('/*') or
                            not media_type.endswith('/')):
                        raise ValueError(
                            'Media type "{}" is not supported for '
                            'requested resource.'.format(media_type)
                        )
                if transfer_syntax_uid is not None:
                    if transfer_syntax_uid != '*':
                        if transfer_syntax_uid not in supported_media_types:
                            raise ValueError(
                                'Transfer syntax "{}" is not supported '
                                'for requested resource.'.format(
                                    transfer_syntax_uid
                                )
                            )
                        expected_media_type = supported_media_types[
                            transfer_syntax_uid
                        ]
                        if expected_media_type != media_type:
                            have_same_type = (
                                self._parse_media_type(media_type)[0] ==
                                self._parse_media_type(expected_media_type)[0]
                            )
                            if (have_same_type and
                                    (media_type.endswith('/*') or
                                        media_type.endswith('/'))):
                                continue
                            raise ValueError(
                                'Transfer syntax "{}" is not supported '
                                'for media type "{}".'.format(
                                    transfer_syntax_uid, media_type
                                )
                            )
                    field_value += '; transfer-syntax={}'.format(
                        transfer_syntax_uid
                    )
            else:
                if media_type not in supported_media_types:
                    raise ValueError(
                        'Media type "{}" is not supported for '
                        'requested resource.'.format(media_type)
                    )
            field_value_parts.append(field_value)
        return ', '.join(field_value_parts)

    def _http_get_multipart_application_dicom(self, url, media_types=None,
                                              params=None):
        '''Performs a HTTP GET request that accepts a multipart message with
        "applicaton/dicom" media type.

        Parameters
        ----------
        url: str
            unique resource locator
        media_types: Tuple[Union[str, Tuple[str, str]]], optional
            acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes, (defaults to
            ``("application/dicom", "1.2.840.10008.1.2.1")``)
        params: Dict[str], optional
            additional HTTP GET query parameters

        Returns
        -------
        List[pydicom.dataset.Dataset]
            DICOM data sets

        '''
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
        resp = self._http_get(url, params, headers)
        datasets = self._decode_multipart_message(resp.content, resp.headers)
        return [pydicom.dcmread(BytesIO(ds)) for ds in datasets]

    def _http_get_multipart_application_octet_stream(self, url,
                                                     media_types=None,  # noqa
                                                     byte_range=None,
                                                     params=None):
        '''Performs a HTTP GET request that accepts a multipart message with
        "applicaton/octet-stream" media type.

        Parameters
        ----------
        url: str
            unique resource locator
        media_types: Tuple[Union[str, Tuple[str, str]]], optional
            acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes (defaults to
            ``("application/octet-stream", "1.2.840.10008.1.2.1")``)
        byte_range: Tuple[int], optional
            start and end of byte range
        params: Dict[str], optional
            additional HTTP GET query parameters

        Returns
        -------
        List[Union[str, bytes]]
            content of HTTP message body parts

        '''
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
        resp = self._http_get(url, params, headers)
        return self._decode_multipart_message(resp.content, resp.headers)

    def _http_get_multipart_image(self, url, media_types,
                                  byte_range=None, params=None, rendered=False):
        '''Performs a HTTP GET request that accepts a multipart message with
        an image media type.

        Parameters
        ----------
        url: str
            unique resource locator
        media_types: Tuple[Union[str, Tuple[str, str]]]
            acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        byte_range: Tuple[int], optional
            start and end of byte range
        params: Dict[str], optional
            additional HTTP GET query parameters
        rendered: bool, optional
            whether resource should be requested using rendered media types

        Returns
        -------
        List[Union[str, bytes]]
            content of HTTP message body parts

        '''
        headers = {}
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
            media_types, supported_media_types
        )
        resp = self._http_get(url, params, headers)
        return self._decode_multipart_message(resp.content, resp.headers)

    def _http_get_multipart_video(self, url, media_types,
                                  byte_range=None, params=None,
                                  rendered=False):
        '''Performs a HTTP GET request that accepts a multipart message with
        a video media type.

        Parameters
        ----------
        url: str
            unique resource locator
        media_types: Tuple[Union[str, Tuple[str, str]]]
            acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        byte_range: Tuple[int], optional
            start and end of byte range
        params: Dict[str], optional
            additional HTTP GET query parameters
        rendered: bool, optional
            whether resource should be requested using rendered media types

        Returns
        -------
        List[Union[str, bytes]]
            content of HTTP message body parts

        '''
        headers = {}
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
            media_types, supported_media_types
        )
        resp = self._http_get(url, params, headers)
        return self._decode_multipart_message(resp.content, resp.headers)

    def _http_get_application_pdf(self, url, params=None):
        '''Performs a HTTP GET request that accepts a message with
        "applicaton/pdf" media type.

        Parameters
        ----------
        url: str
            unique resource locator
        params: Dict[str], optional
            additional HTTP GET query parameters
        rendered: bool, optional
            whether resource should be requested using rendered media types

        Returns
        -------
        Union[str, bytes]
            content of HTTP message body

        '''
        media_type = 'application/pdf'
        resp = self._http_get(url, params, {'Accept': media_type})
        return resp.content

    def _http_get_image(self, url, media_types, params=None):
        '''Performs a HTTP GET request that accepts a message with an image
        media type.

        Parameters
        ----------
        url: str
            unique resource locator
        media_types: Tuple[str]
            image media type (choices: ``"image/jpeg"``, ``"image/gif"``,
            ``"image/jp2"``, ``"image/png"``)
        params: Dict[str], optional
            additional HTTP GET query parameters

        Returns
        -------
        Union[str, bytes]
            content of HTTP message body

        '''
        supported_media_types = {
            'image/',
            'image/*',
            'image/jpeg',
            'image/jp2',
            'image/gif',
            'image/png',
        }
        accept_header_field_value = self._build_accept_header_field_value(
            media_types, supported_media_types
        )
        headers = {
            'Accept': accept_header_field_value,
        }
        resp = self._http_get(url, params, headers)
        return resp.content

    def _http_get_video(self, url, media_types, params=None):
        '''Performs a HTTP GET request that accepts a message with an video
        media type.

        Parameters
        ----------
        url: str
            unique resource locator
        media_types: Tuple[str]
            video media type (choices: ``"video/mpeg"``, ``"video/mp4"``,
            ``"video/H265"``)
        params: Dict[str], optional
            additional HTTP GET query parameters

        Returns
        -------
        Union[str, bytes]
            content of HTTP message body

        '''
        supported_media_types = {
            'video/',
            'video/*',
            'video/mpeg',
            'video/mp4',
            'video/H265',
        }
        accept_header_field_value = self._build_accept_header_field_value(
            media_types, supported_media_types
        )
        headers = {
            'Accept': accept_header_field_value,
        }
        resp = self._http_get(url, params, headers)
        return resp.content

    def _http_get_text(self, url, media_types, params=None):
        '''Performs a HTTP GET request that accepts a message with an text
        media type.

        Parameters
        ----------
        url: str
            unique resource locator
        media_types: Tuple[str]
            text media type (choices: ``"text/html"``, ``"text/plain"``,
            ``"text/xml"``, ``"text/rtf"``)
        params: Dict[str], optional
            additional HTTP GET query parameters

        Returns
        -------
        Union[str, bytes]
            content of HTTP message body

        '''
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
        headers = {
            'Accept': accept_header_field_value,
        }
        resp = self._http_get(url, params, headers)
        return resp.content

    def _http_post(self, url, data, headers):
        '''Performs a HTTP POST request.

        Parameters
        ----------
        url: str
            unique resource locator
        data: Union[str, bytes]
            HTTP request message payload
        headers: Dict[str]
            HTTP request message headers

        Returns
        -------
        requests.models.Response
            HTTP response message

        '''
        logger.debug('POST: {} {}'.format(url, headers))
        response = self._session.post(url=url, data=data, headers=headers)
        logger.debug('request status code: {}'.format(response.status_code))
        response.raise_for_status()
        return response

    def _http_post_multipart_application_dicom(self, url, data):
        '''Performs a HTTP POST request with a multipart payload with
        "application/dicom" media type.

        Parameters
        ----------
        url: str
            unique resource locator
        data: List[bytes]
            DICOM data sets that should be posted

        '''
        content_type = (
            'multipart/related; '
            'type="application/dicom"; '
            'boundary="boundary"'
        )
        content = self._encode_multipart_message(data, content_type)
        self._http_post(url, content, headers={'Content-Type': content_type})

    def search_for_studies(self, fuzzymatching=None, limit=None, offset=None,
                           fields=None, search_filters=None):
        '''Searches for DICOM studies.

        Parameters
        ----------
        fuzzymatching: bool, optional
            whether fuzzy semantic matching should be performed
        limit: int, optional
            maximum number of results that should be returned
        offset: int, optional
            number of results that should be skipped
        fields: List[str], optional
            names of fields (attributes) that should be included in results
        search_filters: dict, optional
            search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match

        Returns
        -------
        List[Dict[str, dict]]
            study representations
            (see `Study Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2>`_)

        Note
        ----
        The server may only return a subset of search results. In this case,
        a warning will notify the client that there are remaining results.
        Remaining results can be requested via repeated calls using the
        `offset` parameter.

        ''' # noqa
        url = self._get_studies_url('qido')
        params = self._parse_qido_query_parameters(
            fuzzymatching, limit, offset, fields, search_filters
        )
        studies = self._http_get_application_json(url, params)
        if studies is None:
            return []
        if not(isinstance(studies, list)):
            studies = [studies]
        return studies

    def _parse_media_type(self, media_type):
        '''Parses media type and extracts its type and subtype.

        Parameters
        ----------
        media_type: str
            media type, e.g., ``"image/jpeg"``

        Returns
        -------
        Tuple[str]
            type and subtype of media type (``("image", "jpeg")``)

        Raises
        ------
        ValueError
            when `media_type` is invalid

        '''
        self._assert_media_type_is_valid(media_type)
        media_type_type, media_type_subtype = media_type.split('/')
        return media_type_type, media_type_subtype

    def _get_common_media_type(self, media_types):
        '''Gets common type of acceptable media types and asserts that only
        one type is specified. For example, ``("image/jpeg", "image/jp2")``
        will pass, but ``("image/jpeg", "video/mpeg2")`` will raise an
        exception.

        Parameters
        ----------
        media_types: Tuple[Union[str, Tuple[str, str]]], optional
            acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        str
            type of media type

        Raises
        ------
        ValueError
            when more than one type is specified

        '''
        if media_types is None:
            raise ValueError('No acceptable media types provided.')
        common_media_types = []
        for item in media_types:
            if isinstance(item, six.string_types):
                media_type = item
            else:
                media_type = item[0]
            if media_type.startswith('application'):
                common_media_types.append(media_type)
            else:
                mtype, msubtype = self._parse_media_type(media_type)
                common_media_types.append('{}/'.format(mtype))
        if len(set(common_media_types)) == 0:
            raise ValueError(
                'No common acceptable media type could be identified.'
            )
        elif len(set(common_media_types)) > 1:
            raise ValueError('Acceptable media types must have the same type.')
        return common_media_types[0]

    def retrieve_bulkdata(self, url, media_types=None, byte_range=None):
        '''Retrieves bulk data from a given location.

        Parameters
        ----------
        url: str
            location of the bulk data
        media_types: Tuple[Union[str, Tuple[str, str]]], optional
            acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes
        byte_range: Tuple[int], optional
            start and end of byte range

        Returns
        -------
        List[bytes]
            bulk data items

        '''
        if media_types is None:
            return self._http_get_multipart_application_octet_stream(
                url, media_types, byte_range
            )
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type == 'application/octet-stream':
            return self._http_get_multipart_application_octet_stream(
                url, media_types, byte_range
            )
        elif common_media_type.startswith('image'):
            return self._http_get_multipart_image(
                url, media_types, byte_range
            )
        else:
            raise ValueError(
                'Media type "{}" is not supported for '
                'retrieval of bulkdata.'.format(common_media_type)
            )

    def retrieve_study(self, study_instance_uid, media_types=None):
        '''Retrieves instances of a given DICOM study.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier
        media_types: Tuple[Union[str, Tuple[str, str]]], optional
            acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        List[pydicom.dataset.Dataset]
            data sets

        '''
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of study.'
            )
        url = self._get_studies_url('wado', study_instance_uid)
        if media_types is None:
            return self._http_get_multipart_application_dicom(url)
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type == 'application/dicom':
            return self._http_get_multipart_application_dicom(
                url, media_types
            )
        elif common_media_type == 'application/octet-stream':
            return self._http_get_multipart_application_octet_stream(
                url, media_types
            )
        elif common_media_type.startswith('image'):
            return self._http_get_multipart_image(
                url, media_types
            )
        elif common_media_type.startswith('video'):
            return self._http_get_multipart_video(
                url, media_types
            )
        else:
            raise ValueError(
                'Media type "{}" is not supported for retrieval '
                'of study.'.format(common_media_type)
            )

    def retrieve_study_metadata(self, study_instance_uid):
        '''Retrieves metadata of instances of a given DICOM study.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier

        Returns
        -------
        Dict[str, dict]
            metadata in DICOM JSON format

        '''
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of '
                'study metadata.'
            )
        url = self._get_studies_url('wado', study_instance_uid)
        url += '/metadata'
        return self._http_get_application_json(url)

    def _assert_uid_format(self, uid):
        '''Checks whether a DICOM UID has the correct format.

        Parameters
        ----------
        uid: str
            DICOM UID

        Raises
        ------
        TypeError
            when `uid` is not a string
        ValueError
            when `uid` doesn't match the regular expression pattern
            ``"^[.0-9]+$"``

        '''
        if not isinstance(uid, six.string_types):
            raise TypeError('DICOM UID must be a string.')
        pattern = re.compile('^[.0-9]+$')
        if not pattern.search(uid):
            raise ValueError('DICOM UID has invalid format.')

    def search_for_series(self, study_instance_uid=None, fuzzymatching=None,
                          limit=None, offset=None, fields=None,
                          search_filters=None):
        '''Searches for DICOM series.

        Parameters
        ----------
        study_instance_uid: str, optional
            unique study identifier
        fuzzymatching: bool, optional
            whether fuzzy semantic matching should be performed
        limit: int, optional
            maximum number of results that should be returned
        offset: int, optional
            number of results that should be skipped
        fields: Union[list, tuple, set], optional
            names of fields (attributes) that should be included in results
        search_filters: Dict[str, Union[str, int, float]], optional
            search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match

        Returns
        -------
        List[Dict[str, dict]]
            series representations
            (see `Series Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2a>`_)

        Note
        ----
        The server may only return a subset of search results. In this case,
        a warning will notify the client that there are remaining results.
        Remaining results can be requested via repeated calls using the
        `offset` parameter.

        ''' # noqa
        if study_instance_uid is not None:
            self._assert_uid_format(study_instance_uid)
        url = self._get_series_url('qido', study_instance_uid)
        params = self._parse_qido_query_parameters(
            fuzzymatching, limit, offset, fields, search_filters
        )
        series = self._http_get_application_json(url, params)
        if series is None:
            return []
        if not(isinstance(series, list)):
            series = [series]
        return series

    def retrieve_series(self, study_instance_uid, series_instance_uid,
                        media_types=None):
        '''Retrieves instances of a given DICOM series.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier
        series_instance_uid: str
            unique series identifier
        media_types: Tuple[Union[str, Tuple[str, str]]], optional
            acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        List[pydicom.dataset.Dataset]
            data sets

        '''
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of series.'
            )
        self._assert_uid_format(study_instance_uid)
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of series.'
            )
        self._assert_uid_format(series_instance_uid)
        url = self._get_series_url(
            'wado', study_instance_uid, series_instance_uid
        )
        if media_types is None:
            return self._http_get_multipart_application_dicom(url)
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type == 'application/dicom':
            return self._http_get_multipart_application_dicom(
                url, media_types
            )
        elif common_media_type == 'application/octet-stream':
            return self._http_get_multipart_application_octet_stream(
                url, media_types
            )
        elif common_media_type.startswith('image'):
            return self._http_get_multipart_image(
                url, media_types
            )
        elif common_media_type.startswith('video'):
            return self._http_get_multipart_video(
                url, media_types
            )
        else:
            raise ValueError(
                'Media type "{}" is not supported for retrieval '
                'of series.'.format(common_media_type)
            )

    def retrieve_series_metadata(self, study_instance_uid, series_instance_uid):
        '''Retrieves metadata for instances of a given DICOM series.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier
        series_instance_uid: str
            unique series identifier

        Returns
        -------
        Dict[str, dict]
            metadata in DICOM JSON format

        '''
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
        self._assert_uid_format(series_instance_uid)
        url = self._get_series_url(
            'wado', study_instance_uid, series_instance_uid
        )
        url += '/metadata'
        return self._http_get_application_json(url)

    def retrieve_series_rendered(self, study_instance_uid,
                                 series_instance_uid,
                                 media_types=None,
                                 params=None):
        '''Retrieves an individual, server-side rendered DICOM series.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier
        series_instance_uid: str
            unique series identifier
        media_types: Tuple[Union[str, Tuple[str]]], optional
            acceptable media types (choices: ``"image/jpeg"``, ``"image/jp2"``,
            ``"image/gif"``, ``"image/png"``, ``"video/gif"``, ``"video/mp4"``,
            ``"video/h265"``, ``"text/html"``, ``"text/plain"``,
            ``"text/xml"``, ``"text/rtf"``, ``"application/pdf"``)
        params: Dict[str], optional
            additional parameters relevant for given `media_type`,
            e.g., ``{"quality": 95}`` for ``"image/jpeg"``

        Returns
        -------
        bytes
            rendered series

        '''
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
        url = self._get_series_url(
            'wado', study_instance_uid, series_instance_uid
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
                'Media type "{}" is not supported for '
                'retrieval of rendered series.'.format(common_media_type)
            )

    def search_for_instances(self, study_instance_uid=None,
                             series_instance_uid=None, fuzzymatching=None,
                             limit=None, offset=None, fields=None,
                             search_filters=None):
        '''Searches for DICOM instances.

        Parameters
        ----------
        study_instance_uid: str, optional
            unique study identifier
        series_instance_uid: str, optional
            unique series identifier
        fuzzymatching: bool, optional
            whether fuzzy semantic matching should be performed
        limit: int, optional
            maximum number of results that should be returned
        offset: int, optional
            number of results that should be skipped
        fields: Union[list, tuple, set], optional
            names of fields (attributes) that should be included in results
        search_filters: Dict[str, Union[str, int, float]], optional
            search filter criteria as key-value pairs, where *key* is a keyword
            or a tag of the attribute and *value* is the expected value that
            should match

        Returns
        -------
        List[Dict[str, dict]]
            instance representations
            (see `Instance Result Attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2b>`_)

        Note
        ----
        The server may only return a subset of search results. In this case,
        a warning will notify the client that there are remaining results.
        Remaining results can be requested via repeated calls using the
        `offset` parameter.

        ''' # noqa
        if study_instance_uid is not None:
            self._assert_uid_format(study_instance_uid)
        url = self._get_instances_url(
            'qido', study_instance_uid, series_instance_uid
        )
        params = self._parse_qido_query_parameters(
            fuzzymatching, limit, offset, fields, search_filters
        )
        instances = self._http_get_application_json(url, params)
        if instances is None:
            return []
        if not(isinstance(instances, list)):
            instances = [instances]
        return instances

    def retrieve_instance(self, study_instance_uid, series_instance_uid,
                          sop_instance_uid, media_types=None):
        '''Retrieves an individual DICOM instance.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier
        series_instance_uid: str
            unique series identifier
        sop_instance_uid: str
            unique instance identifier
        media_types: Tuple[Union[str, Tuple[str, str]]], optional
            acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        pydicom.dataset.Dataset
            data set

        '''

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
        self._assert_uid_format(sop_instance_uid)
        url = self._get_instances_url(
            'wado', study_instance_uid, series_instance_uid, sop_instance_uid
        )
        if media_types is None:
            return self._http_get_multipart_application_dicom(url)[0]
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type == 'application/dicom':
            return self._http_get_multipart_application_dicom(
                url, media_types
            )[0]
        elif common_media_type == 'application/octet-stream':
            return self._http_get_multipart_application_octet_stream(
                url, media_types
            )[0]
        elif common_media_type.startswith('image'):
            frames = self._http_get_multipart_image(
                url, media_types
            )
            if len(frames) > 1:
                return frames
            else:
                return frames[0]
        elif common_media_type.startswith('video'):
            frames = self._http_get_multipart_video(
                url, media_types
            )
            if len(frames) > 1:
                return frames
            else:
                return frames[0]
        else:
            raise ValueError(
                'Media type "{}" is not supported for retrieval '
                'of instance.'.format(common_media_type)
            )

    def store_instances(self, datasets, study_instance_uid=None):
        '''Stores DICOM instances.

        Parameters
        ----------
        datasets: List[pydicom.dataset.Dataset]
            instances that should be stored
        study_instance_uid: str, optional
            unique study identifier

        '''
        url = self._get_studies_url('stow', study_instance_uid)
        encoded_datasets = list()
        # TODO: can we do this more memory efficient? Concatenations?
        for ds in datasets:
            with BytesIO() as b:
                pydicom.dcmwrite(b, ds)
                encoded_datasets.append(b.getvalue())
        self._http_post_multipart_application_dicom(url, encoded_datasets)

    def retrieve_instance_metadata(self, study_instance_uid,
                                   series_instance_uid, sop_instance_uid):
        '''Retrieves metadata of an individual DICOM instance.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier
        series_instance_uid: str
            unique series identifier
        sop_instance_uid: str
            unique instance identifier

        Returns
        -------
        Dict[str, dict]
            metadata in DICOM JSON format

        '''
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
            'wado', study_instance_uid, series_instance_uid, sop_instance_uid
        )
        url += '/metadata'
        return self._http_get_application_json(url)

    def retrieve_instance_rendered(self, study_instance_uid,
                                   series_instance_uid,
                                   sop_instance_uid,
                                   media_types=None,
                                   params=None):
        '''Retrieves an individual, server-side rendered DICOM instance.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier
        series_instance_uid: str
            unique series identifier
        sop_instance_uid: str
            unique instance identifier
        media_types: Tuple[union[str, Tuple[str]]], optional
            acceptable media types (choices: ``"image/jpeg"``, ``"image/jp2"``,
            ``"image/gif"``, ``"image/png"``, ``"video/gif"``, ``"video/mp4"``,
            ``"video/h265"``, ``"text/html"``, ``"text/plain"``,
            ``"text/xml"``, ``"text/rtf"``, ``"application/pdf"``)
        params: Dict[str], optional
            additional parameters relevant for given `media_type`,
            e.g., ``{"quality": 95}`` for ``"image/jpeg"``

        Returns
        -------
        bytes
            rendered instance

        '''
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
            'wado', study_instance_uid, series_instance_uid, sop_instance_uid
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
                'Media type "{}" is not supported for '
                'retrieval of rendered instance.'.format(common_media_type)
            )

    def retrieve_instance_frames(self, study_instance_uid, series_instance_uid,
                                 sop_instance_uid, frame_numbers,
                                 media_types=None):
        '''Retrieves one or more frames of an individual DICOM instance.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier
        series_instance_uid: str
            unique series identifier
        sop_instance_uid: str
            unique instance identifier
        frame_numbers: List[int]
            one-based positional indices of the frames within the instance
        media_types: Tuple[Union[str, Tuple[str, str]]], optional
            acceptable media types and optionally the UIDs of the
            corresponding transfer syntaxes

        Returns
        -------
        List[bytes]
            pixel data for each frame

        '''
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
            'wado', study_instance_uid, series_instance_uid, sop_instance_uid
        )
        frame_list = ','.join([str(n) for n in frame_numbers])
        url += '/frames/{frame_list}'.format(frame_list=frame_list)
        if media_types is None:
            return self._http_get_multipart_application_octet_stream(url)
        common_media_type = self._get_common_media_type(media_types)
        if common_media_type == 'application/octet-stream':
            return self._http_get_multipart_application_octet_stream(
                url, media_types
            )
        elif common_media_type.startswith('image'):
            return self._http_get_multipart_image(url, media_types)
        elif common_media_type.startswith('video'):
            return self._http_get_multipart_video(url, media_types)
        else:
            raise ValueError(
                'Media type "{}" is not supported for '
                'retrieval of frames.'.format(common_media_type)
            )

    def retrieve_instance_frames_rendered(self, study_instance_uid,
                                          series_instance_uid,
                                          sop_instance_uid,
                                          frame_number,
                                          media_types=None,
                                          params=None):
        '''Retrieves one or more server-side rendered frames of an
        individual DICOM instance.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier
        series_instance_uid: str
            unique series identifier
        sop_instance_uid: str
            unique instance identifier
        frame_number: int
            one-based positional index of the frame within the instance
        media_types: Tuple[Union[str, Tuple[str]]], optional
            acceptable media type (choices: ``"image/jpeg"``, ``"image/jp2"``,
            ``"image/gif"``, ``"image/png"``)
        params: Dict[str], optional
            additional parameters relevant for given `media_type`,
            e.g., ``{"quality": 95}`` for ``"image/jpeg"`` media type

        Returns
        -------
        bytes
            rendered frames

        Note
        ----
        Not all media types are compatible with all SOP classes.

        '''
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
            'wado', study_instance_uid, series_instance_uid, sop_instance_uid
        )
        url += '/frames/{frame_number}/rendered'.format(
            frame_number=frame_number
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
                'Media type "{}" is not supported for '
                'retrieval of rendered frame.'.format(common_media_type)
            )

    @staticmethod
    def lookup_keyword(tag):
        '''Looks up the keyword of a DICOM attribute.

        Parameters
        ----------
        tag: Union[str, int, Tuple[str], pydicom.tag.Tag]
            attribute tag (e.g. ``"00080018"``)

        Returns
        -------
        str
            attribute keyword (e.g. ``"SOPInstanceUID"``)

        '''
        return pydicom.datadict.keyword_for_tag(tag)

    @staticmethod
    def lookup_tag(keyword):
        '''Looks up the tag of a DICOM attribute.

        Parameters
        ----------
        keyword: str
            attribute keyword (e.g. ``"SOPInstanceUID"``)

        Returns
        -------
        str
            attribute tag as HEX string (e.g. ``"00080018"``)

        '''
        tag = pydicom.datadict.tag_for_keyword(keyword)
        tag = pydicom.tag.Tag(tag)
        return '{0:04x}{1:04x}'.format(tag.group, tag.element).upper()
