'''Active Programming Interface (API).'''
import re
import os
import sys
import logging
import email
import six
from urllib3.filepost import choose_boundary
from io import BytesIO
from collections import OrderedDict
if sys.version_info.major < 3:
    from urllib import quote_plus
else:
    from urllib.parse import quote_plus

import requests
import pydicom

from dicomweb_client.error import DICOMJSONError


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
                        logger.warn(
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
        # http://dicom.nema.org/medical/dicom/current/output/html/part18.html#sect_F.2.2
        elem_value = []
        for v in value:
            if not isinstance(v, dict):
                # Some DICOMweb services get this wrong, so we workaround it
                # and issue a warning rather than raising an error.
                logger.warn(
                    'attribute with VR Person Name (PN) is not '
                    'formatted correctly'
                )
                elem_value.append(v)
            else:
                # TODO: How to handle "Ideographic" and "Phonetic"?
                elem_value.append(v['Alphabetic'])
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
        # TODO: check if mandatory
        logger.warn('missing value for data element "{}"'.format(tag))
    try:
        return pydicom.dataelem.DataElement(tag=tag, value=elem_value, VR=vr)
    except Exception as e:
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
            logger.warn(
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
                 qido_url_prefix=None, wado_url_prefix=None,
                 stow_url_prefix=None):
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
            path to CA bundle file in Privacy Enhanced Mail (PEM) format
        qido_url_prefix: str, optional
            URL path prefix for QIDO RESTful services
        wado_url_prefix: str, optional
            URL path prefix for WADO RESTful services
        stow_url_prefix: str, optional
            URL path prefix for STOW RESTful services

        '''
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
        # This regular expressin extracts the scheme and host name from the URL
        # and optionally the port number and prefix:
        # <scheme>://<host>(:<port>)(/<prefix>)
        # For example: "https://mydomain.com:80/wado-rs", where
        # scheme="https", host="mydomain.com", port=80, prefix="wado-rs"
        pattern = re.compile(
            '(?P<scheme>[https]+)://(?P<host>[^/:]+)'
            '(?::(?P<port>\d+))?(?:(?P<prefix>/\w+))?'
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
        self.url_prefix = match.group('prefix')
        if self.url_prefix is None:
            self.url_prefix = ''
        self._session.headers.update({'Host': self.host})
        if username is not None:
            if not password:
                raise ValueError(
                    'No password provided for user "{0}".'.format(username)
                )
            self._session.auth = (username, password)

    def _parse_query_parameters(self, fuzzymatching, limit, offset,
                                fields, **search_filters):
        params = dict()
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
            params['includefield'] = list()
            for field in set(fields):
                if not(isinstance(field, str)):
                    raise TypeError('Elements of "fields" must be a string.')
                params['includefield'].append(field)
        for field, criterion in search_filters.items():
            if not(isinstance(field, str)):
                raise TypeError('Keys of "search_filters" must be strings.')
            # TODO: datetime?
            params[field] = criterion
        # Sort query parameters to facilitate unit testing
        return OrderedDict(sorted(params.items()))

    def _get_service_url(self, service):
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
        if study_instance_uid is not None:
            url = self._get_studies_url(service, study_instance_uid)
            if series_instance_uid is not None:
                url += '/series/{series_instance_uid}'
            else:
                url += '/series'
        else:
            if series_instance_uid is not None:
                logger.warn(
                    'series UID is ignored because study UID is undefined'
                )
            url = '{service_url}/series'
        service_url = self._get_service_url(service)
        return url.format(
            service_url=service_url, series_instance_uid=series_instance_uid
        )

    def _get_instances_url(self, service, study_instance_uid=None,
                           series_instance_uid=None, sop_instance_uid=None):
        if study_instance_uid is not None and series_instance_uid is not None:
            url = self._get_series_url(
                service, study_instance_uid, series_instance_uid
            )
            url += '/instances'
            if sop_instance_uid is not None:
                url += '/{sop_instance_uid}'
        else:
            if sop_instance_uid is not None:
                logger.warn(
                    'SOP Instance UID is ignored because Study/Series '
                    'Instance UID are undefined'
                )
            url = '{service_url}/instances'
        service_url = self._get_service_url(service)
        return url.format(
            service_url=service_url, sop_instance_uid=sop_instance_uid
        )

    @staticmethod
    def _build_query_string(params):
        components = []
        for key, value in params.items():
            if isinstance(value, (list, tuple, set)):
                for v in value:
                    c = '='.join([key, quote_plus(str(v))])
                    components.append(c)
            else:
                c = '='.join([key, quote_plus(str(value))])
                components.append(c)
        return '?{}'.format('&'.join(components))

    def _http_get(self, url, params, headers):
        '''Performs a HTTP GET request.

        Parameters
        ----------
        url: str
            unique resource locator
        params: Dict[str]
            query parameters
        headers: Dict[str]
            HTTP request message headers

        Returns
        -------
        requests.models.Response
            HTTP response message

        '''
        url += self._build_query_string(params)
        logger.debug('GET: {}'.format(url))
        resp = self._session.get(url=url, headers=headers)
        resp.raise_for_status()
        return resp

    def _http_get_application_json(self, url, **params):
        '''Performs a HTTP GET request that accepts "applicaton/dicom+json"
        media type.

        Parameters
        ----------
        url: str
            unique resource locator
        params: Dict[str]
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

    def _http_get_application_dicom(self, url, **params):
        '''Performs a HTTP GET request that accepts "applicaton/dicom"
        media type.

        Parameters
        ----------
        url: str
            unique resource locator
        params: Dict[str]
            query parameters

        Returns
        -------
        pydicom.dataset.Dataset
            DICOM data set

        '''
        content_type = 'application/dicom'
        resp = self._http_get(url, params, {'Accept': content_type})
        return pydicom.dcmread(BytesIO(resp.content))

    @staticmethod
    def _decode_multipart_message(body, headers):
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

    @staticmethod
    def _encode_multipart_message(data, content_type):
        '''Extracts parts of a HTTP multipart response message.

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

    def _http_get_multipart_application_dicom(self, url, **params):
        '''Performs a HTTP GET request that accepts a multipart message with
        "applicaton/dicom" media type.

        Parameters
        ----------
        url: str
            unique resource locator
        params: Dict[str]
            query parameters

        Returns
        -------
        List[pydicom.dataset.Dataset]
            DICOM data sets

        '''
        content_type = (
            'multipart/related; type="application/dicom"; '
            'boundary="--{boundary}"'.format(boundary=choose_boundary())
        )
        resp = self._http_get(url, params, {'Accept': content_type})
        datasets = self._decode_multipart_message(resp.content, resp.headers)
        return [pydicom.dcmread(BytesIO(ds)) for ds in datasets]

    def _http_get_multipart_application_octet_stream(self, url, **params):
        '''Performs a HTTP GET request that accepts a multipart message with
        "applicaton/octet-stream" media type.

        Parameters
        ----------
        url: str
            unique resource locator
        params: Dict[str]
            query parameters

        Returns
        -------
        List[Union[str, bytes]]
            content of HTTP message body parts

        '''
        content_type = (
            'multipart/related; type="application/octet-stream"; '
            'boundary="--{boundary}"'.format(boundary=choose_boundary())
        )
        resp = self._http_get(url, params, {'Accept': content_type})
        return self._decode_multipart_message(resp.content, resp.headers)

    def _http_get_multipart_image(self, url, image_format, **params):
        '''Performs a HTTP GET request that accepts a multipart message with
        "image/{image_format}" media type.

        Parameters
        ----------
        url: str
            unique resource locator
        image_format: str
            image format
        params: Dict[str]
            query parameters

        Returns
        -------
        List[Union[str, bytes]]
            content of HTTP message body parts

        '''
        if image_format not in {'jpeg', 'x-jls', 'jp2'}:
            raise ValueError(
                'Image format "{}" is not supported.'.format(image_format)
            )
        content_type = (
            'multipart/related; type="image/{image_format}"; '
            'boundary="--{boundary}"'.format(
                image_format=image_format, boundary=choose_boundary()
            )
        )
        resp = self._http_get(url, params, {'Accept': content_type})
        return self._decode_multipart_message(resp.content, resp.headers)

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
        logger.debug('POST: {}'.format(url))
        response = self._session.post(url=url, data=data, headers=headers)
        response.raise_for_status()
        return response

    def _http_post_multipart_application_dicom(self, url, data):
        '''Performs a HTTP POST request

        Parameters
        ----------
        url: str
            unique resource locator
        data: List[bytes]
            DICOM data sets that should be posted
        datasets: List[pydicom.dataset.Dataset]
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
                           fields=None, search_filters={}):
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
            (see `returned attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2>`_)

        ''' # noqa
        url = self._get_studies_url('qido')
        params = self._parse_query_parameters(
            fuzzymatching, limit, offset, fields, **search_filters
        )
        studies = self._http_get_application_json(url, **params)
        if studies is None:
            return []
        if not(isinstance(studies, list)):
            studies = [studies]
        return studies

    def retrieve_bulkdata(self, url, image_format=None,
                          image_params={'quality': 95}):
        '''Retrieves bulk data from a given location.

        Parameters
        ----------
        url: str
            unique resource location of bulk data as obtained from a metadata
            request, for example
        image_format: str, optional
            name of the image format for media type ``"image/{image_format}"``;
            if ``None`` data will be requested uncompressed using
            ``"application/octet-stream"`` media type
        image_params: Dict[str], optional
            additional parameters relevant for `image_format`

        Returns
        -------
        List[bytes]
            bulk data items

        '''
        if image_format is None:
            return self._http_get_multipart_application_octet_stream(url)
        else:
            return self._http_get_multipart_image(
                url, image_format, **image_params
            )

    def retrieve_study(self, study_instance_uid):
        '''Retrieves instances of a given DICOM study.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier

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
        return self._http_get_multipart_application_dicom(url)

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

    def _check_uid_format(self, uid):
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
                          search_filters={}):
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
            (see `returned attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2a>`_)

        ''' # noqa
        if study_instance_uid is not None:
            self._check_uid_format(study_instance_uid)
        url = self._get_series_url('qido', study_instance_uid)
        params = self._parse_query_parameters(
            fuzzymatching, limit, offset, fields, **search_filters
        )
        series = self._http_get_application_json(url, **params)
        if series is None:
            return []
        if not(isinstance(series, list)):
            series = [series]
        return series

    def retrieve_series(self, study_instance_uid, series_instance_uid):
        '''Retrieves instances of a given DICOM series.

        Parameters
        ----------
        study_instance_uid: str
            unique study identifier
        series_instance_uid: str
            unique series identifier

        Returns
        -------
        List[pydicom.dataset.Dataset]
            data sets

        '''
        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of series.'
            )
        self._check_uid_format(study_instance_uid)
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of series.'
            )
        self._check_uid_format(series_instance_uid)
        url = self._get_series_url(
            'wado', study_instance_uid, series_instance_uid
        )
        return self._http_get_multipart_application_dicom(url)

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
        self._check_uid_format(study_instance_uid)
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of '
                'series metadata.'
            )
        self._check_uid_format(series_instance_uid)
        url = self._get_series_url(
            'wado', study_instance_uid, series_instance_uid
        )
        url += '/metadata'
        return self._http_get_application_json(url)

    def search_for_instances(self, study_instance_uid=None,
                             series_instance_uid=None, fuzzymatching=None,
                             limit=None, offset=None, fields=None,
                             search_filters={}):
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
            (see `returned attributes <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/sect_6.7.html#table_6.7.1-2b>`_)

        ''' # noqa
        if study_instance_uid is not None:
            self._check_uid_format(study_instance_uid)
        url = self._get_instances_url(
            'qido', study_instance_uid, series_instance_uid
        )
        params = self._parse_query_parameters(
            fuzzymatching, limit, offset, fields, **search_filters
        )
        instances = self._http_get_application_json(url, **params)
        if instances is None:
            return []
        if not(isinstance(instances, list)):
            instances = [instances]
        return instances

    def retrieve_instance(self, study_instance_uid, series_instance_uid,
                          sop_instance_uid):
        '''Retrieves an individual DICOM instance.

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
        pydicom.dataset.Dataset
            data set

        '''

        if study_instance_uid is None:
            raise ValueError(
                'Study Instance UID is required for retrieval of instance.'
            )
        self._check_uid_format(study_instance_uid)
        if series_instance_uid is None:
            raise ValueError(
                'Series Instance UID is required for retrieval of instance.'
            )
        self._check_uid_format(series_instance_uid)
        if sop_instance_uid is None:
            raise ValueError(
                'SOP Instance UID is required for retrieval of instance.'
            )
        self._check_uid_format(sop_instance_uid)
        url = self._get_instances_url(
            'wado', study_instance_uid, series_instance_uid, sop_instance_uid
        )
        return self._http_get_multipart_application_dicom(url)[0]

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

    def retrieve_instance_frames(self, study_instance_uid, series_instance_uid,
                                 sop_instance_uid, frame_numbers,
                                 image_format=None,
                                 image_params={'quality': 95}):
        '''Retrieves uncompressed frame items of a pixel data element of an
        individual DICOM image instance.

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
        image_format: str, optional
            name of the image format; if ``None`` pixel data will be requested
            uncompressed as ``"application/octet-stream"``
            (default:``None``, options: ``{"jpeg", "x-jls", "jp2"}``)
        image_params: Dict[str], optional
            additional parameters relevant for `image_format`

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
        if image_format is None:
            pixeldata = self._http_get_multipart_application_octet_stream(url)
            # To interpret the raw pixel data, one would need additional
            # metadata, such as the dimensions of the image and its
            # photometric interpretation.
            return pixeldata
        else:
            return self._http_get_multipart_image(
                url, image_format, **image_params
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
        return '{0:04x}{1:04x}'.format(tag.group, tag.element)
