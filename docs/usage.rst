.. _user-guide:

User guide
==========

The client can be used with any DICOMweb server, such as `dcm4che <http://www.dcm4che.org/>`_, `orthanc <https://www.orthanc-server.com/static.php?page=dicomweb>`_ or `DICOMcloud <https://dicomcloud.github.io/>`_.

.. _api:

Application Programming Interface (API)
---------------------------------------

Interacting with a remote DICOMweb server
+++++++++++++++++++++++++++++++++++++++++

To interact with a publicly accessible server, you only need to provide the ``url`` for the server address.

.. code-block:: python

    from dicomweb_client.api import DICOMwebClient

    client = DICOMwebClient(url="https://mydicomwebserver.com")


Some servers expose the different DICOMweb RESTful services using different path prefixes.
For example, the publicly accessible `DICOMcloud server <https://dicomcloud.azurewebsites.net>`_ uses the prefixes ``"qidors"``, ``"wadors"``, and ``"stowrs"`` for QIDO-RS, WADO-RS, and STOW-RS, respectively.
You can specify these prefixes using ``qido_url_prefix``, ``wado_url_prefix``, and ``stow_url_prefix``.

.. code-block:: python

    from dicomweb_client.api import DICOMwebClient

    client = DICOMwebClient(
        url="https://dicomcloud.azurewebsites.net",
        qido_url_prefix="qidors",
        wado_url_prefix="wadors",
        stow_url_prefix="stowrs"
    )

.. _auth:

Authentication and authorization
++++++++++++++++++++++++++++++++

To interact with servers requiring authentication, ``DICOMwebClient`` accepts arbitrary authentication handlers derived from ``requests.auth.AuthBase`` (see `here <http://docs.python-requests.org/en/master/user/authentication/>`_ for details).

.. code-block:: python

    from requests.auth import HTTPBasicAuth
    from dicomweb_client.api import DICOMwebClient
    from dicomweb_client.session_utils import create_session_from_auth

    auth = HTTPBasicAuth('myusername', 'mypassword')
    session = create_session_from_auth(auth)

    client = DICOMwebClient(
        url="https://mydicomwebserver.com",
        session=session
    )

To simplify usage for basic HTTP authentication, you may also directly provide a username and password using the corresponding arguments.

.. code-block:: python

    from dicomweb_client.api import DICOMwebClient
    from dicomweb_client.session_utils import create_session_from_user_pass

    session = create_session_from_user_pass(
        username='myusername',
        password='mypassword'
    )

    client = DICOMwebClient(
        url="https://mydicomwebserver.com",
        session=session
    )

To interact with servers supporting token-based authorization, you can provide the access token using the ``headers`` argument (the header will be included in every client request message).

.. code-block:: python

    from dicomweb_client.api import DICOMwebClient

    access_token = "mytoken"
    client = DICOMwebClient(
        url="https://mydicomwebserver.com",
        headers={"Authorization": "Bearer {}".format(access_token)}
    )


To interact with servers requiring certificate-based authentication, you can provide the CA bundle and client certificate using the ``ca_bundle`` and ``cert`` arguments, respectively.

.. code-block:: python

    from dicomweb_client.api import DICOMwebClient
    from dicomweb_client.session_utils import (
        create_session,
        add_certs_to_session
    )

    session = create_session()
    session = add_certs_to_session(
        session=session,
        ca_bundle="/path/to/ca.crt",
        cert="/path/to/cert.pem"
    )

    client = DICOMwebClient(url="https://mydicomwebserver.com")


To interact with a server of the Google Healthcare API requiring OpenID Connect based authentication and authorization, provide a session authenticated using the Google Cloud Platform (GCP) credentials.
See `GCP documentation <https://cloud.google.com/docs/authentication/production>`_ for details.

Note that GCP authentication requires installation of the package distribution with the ``gcp`` extra requirements: ``$ pip install dicomweb-client[gcp]``.

.. code-block:: python

    from dicomweb_client.api import DICOMwebClient
    from dicomweb_client.session_utils import create_session_from_gcp_credentials

    session = create_session_from_gcp_credentials()

    client = DICOMwebClient(
        url="https://mydicomwebserver.com",
        session=session
    )

Accessing local DICOM Part10 files
++++++++++++++++++++++++++++++++++

The package provides the :class:`dicomweb_client.api.DICOMfileClient` class for accessing data stored as DICOM Part10 files on a file system.
The class exposes the same :class:`dicomweb_client.api.DICOMClient` interface as the :class:`dicomweb_client.api.DICOMwebClient` and can be used as a drop-in replacement.

.. code-block:: python

    from dicomweb_client.api import DICOMfileClient

    client = DICOMfileClient("/path/to/directory")


.. _storeinstances:

STOW-RS StoreInstances
++++++++++++++++++++++

Store a single dataset obtained from a PS3.10 file:

.. code-block:: python

    import pydicom

    filename = "/path/to/file.dcm"
    dataset = pydicom.dcmread(filename)

    client.store_instances(datasets=[dataset])


.. _searchforstudies:

QIDO-RS SeachForStudies
+++++++++++++++++++++++

Search for all studies (up to server-defined maximum set per call - see below to iteratively get all studies):

.. code-block:: python

    studies = client.search_for_studies()


Search for studies filtering by *PatientID*:

.. code-block:: python

    studies = client.search_for_studies(search_filters={'PatientID': 'ABC123'})


Note that attributes can be specified in ``search_filters`` using either the keyword or the tag:

.. code-block:: python

    studies = client.search_for_studies(search_filters={'00100020': 'ABC123'})

Search for all studies but limit the number of returned results using the ``limit`` parameter.

.. code-block:: python

    studies_subset = client.search_for_studies(limit=100)

A server may also automatically limit the number of results that it returns per search request.
In this case, the method can be called repeatedly to request remaining results using the ``offset`` parameter.

.. code-block:: python

    studies = []
    offset = 0
    while True:
        subset = client.search_for_studies(offset=offset)
        if len(subset) == 0:
            break
        studies.extend(subset)
        offset += len(subset)


.. _searchforseries:

QIDO-RS SeachForSeries
++++++++++++++++++++++

Search for all series:

.. code-block:: python

    series = client.search_for_series()


Search for series of a given study:

.. code-block:: python

    series = client.search_for_series('1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639')


Search for series filtering by *AccessionNumber*:

.. code-block:: python

    series = client.search_for_series(search_filters={'AccessionNumber': '123456'})


Search for series filtering by *AccessionNumber* (using wildcard ``?`` to match a range of numbers):

.. code-block:: python

    series = client.search_for_series(search_filters={'AccessionNumber': '12345?'})


Search for series filtering by *SeriesDescription*:

.. code-block:: python

    series = client.search_for_series(search_filters={'SeriesDescription': 'T2 AXIAL'})


Search for series filtering by *SeriesDescription* (using wildcard ``*`` to match a range of descriptions):

.. code-block:: python

    series = client.search_for_series(search_filters={'SeriesDescription': 'T2 AX*'})


Search for series filtering by *Modality*:

.. code-block:: python

    series = client.search_for_series(search_filters={'Modality': 'CT'})


.. _searchforinstances:

QIDO-RS SeachForInstances
+++++++++++++++++++++++++

Search for all instances:

.. code-block:: python

    instances = client.search_for_instances()


Search for instances of a given study and series:

.. code-block:: python

    instances = client.search_for_instances(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034'
    )


Search for instances filtering by *SOPClassUID*:

.. code-block:: python

    instances = client.search_for_instances(search_filters={'SOPClassUID': '1.2.840.10008.5.1.4.1.1.2'})


.. _retrievestudy:

WADO-RS RetrieveStudy
+++++++++++++++++++++

Retrieve instances of a given study:

.. code-block:: python

    instances = client.retrieve_study('1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639')


.. _retrieveseries:

WADO-RS RetrieveSeries
++++++++++++++++++++++

Retrieve instances of a given series:

.. code-block:: python

    instances = client.retrieve_series(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034'
    )

Retrieve full instances of a given series using specific JPEG 2000 transfer syntax for encoding of bulk data:

.. code-block:: python

    instance = client.retrieve_instance(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034',
        media_types=(('application/dicom', '1.2.840.10008.1.2.4.90', ), )
    )

Retrieve bulk data of instances of a given series using specific JPEG 2000 transfer syntax:

.. code-block:: python

    instance = client.retrieve_instance(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034',
        media_types=(('image/jp2', '1.2.840.10008.1.2.4.90', ), )
    )


.. _retrieveinstance:

WADO-RS RetrieveInstance
++++++++++++++++++++++++

Retrieve full instance using default Explicit VR Little Endian transfer syntax for encoding of bulk data:

.. code-block:: python

    instance = client.retrieve_instance(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034'
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534'
    )


Retrieve full instance using specific JPEG 2000 transfer syntax for encoding of bulk data:

.. code-block:: python

    instance = client.retrieve_instance(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034'
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534',
        media_types=(('application/dicom', '1.2.840.10008.1.2.4.90', ), )
    )

Retrieve bulk data of instance using specific JPEG 2000 transfer syntax:

.. code-block:: python

    instance = client.retrieve_instance(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034'
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534',
        media_types=(('image/jp2', '1.2.840.10008.1.2.4.90', ), )
    )

.. _retrievemetadata:

WADO-RS RetrieveMetadata
++++++++++++++++++++++++


Retrieve metadata for instances of a given study:

.. code-block:: python

    metadata = client.retrieve_study_metadata('1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639')


Retrieve metadata for instances of a given series:

.. code-block:: python

    metadata = client.retrieve_series_metadata(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034'
    )

Retrieve metadata for a particular instance:

.. code-block:: python

    metadata = client.retrieve_instance_metadata(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034',
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534'
    )

.. note::

    WADO-RS RetrieveMetadata always returns metadata at the instance-level, ``retrieve_study_metadata()`` and ``retrieve_series_metadata()`` return an array of metadata items for each instance belonging to a given study and series, respectively.


.. _retrieveframes:

WADO-RS RetrieveFrames
++++++++++++++++++++++

Retrieve a set of frames with default transfer syntax ("application/octet-stream"):

.. code-block:: python

    frames = client.retrieve_instance_frames(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034',
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534',
        frame_numbers=[1, 2]
    )

Retrieve a set of frames of a given instances as JPEG compressed image:

.. code-block:: python

    frames = client.retrieve_instance_frames(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034',
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534',
        frame_numbers=[1, 2],
        media_types=('image/jpeg', )
    )

Retrieve a set of frames of a given instances as compressed image in any available format:

.. code-block:: python

    frames = client.retrieve_instance_frames(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034',
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534',
        frame_numbers=[1, 2],
        media_types=('image/*', )
    )

Retrieve a set of frames of a given instances as either JPEG 2000 or JPEG-LS compressed image:

.. code-block:: python

    frames = client.retrieve_instance_frames(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034',
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534',
        frame_numbers=[1, 2],
        media_types=('image/jp2', 'image/x-jpls', )
    )

Retrieve a set of frames of a given instances as either JPEG, JPEG 2000 or JPEG-LS lossless compressed image using specific transfer syntaxes:

.. code-block:: python

    frames = client.retrieve_instance_frames(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034',
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534',
        frame_numbers=[1, 2],
        media_types=(
            ('image/jpeg', '1.2.840.10008.1.2.4.57', ),
            ('image/jp2', '1.2.840.10008.1.2.4.90', ),
            ('image/x-jpls', '1.2.840.10008.1.2.4.80', ),
        )
    )

.. _retrievebulkdata:

WADO-RS RetrieveBulkdata
++++++++++++++++++++++++

Retrieve bulk data given a URL:

.. code-block:: python

    data = client.retrieve_bulkdata('https://mydicomwebserver.com/studies/...')


.. _retrieverenderedtransaction:

WADO-RS RetrieveRenderedTransaction
+++++++++++++++++++++++++++++++++++

Retrieve a single-frame image instance rendered as a PNG compressed image:

.. code-block:: python

    frames = client.retrieve_instance_rendered(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034',
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534',
        media_types=('image/png', )
    )

Retrieve a single frame of a multi-frame image instance rendered as a high-quality JPEG compressed image that includes an ICC profile:

.. code-block:: python

    frames = client.retrieve_instance_frames_rendered(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034',
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534',
        frame_numbers=[1],
        media_types=('image/jpeg', ),
        params={'quality': 95, 'iccprofile': 'yes'}
    )

When frames are retrieved in image format, they can be converted into a *NumPy* array using the *PIL* module:

.. code-block:: python

    from io import BytesIO

    import numpy as np
    from PIL import Image

    image = Image.open(BytesIO(frames[0]))
    array = np.array(image)


.. warning::

    Retrieving images using lossy compression methods may lead to image recompression artifacts if the images have been stored lossy compressed.

.. _cli:

Loading JSON Data To ``pydicom``
++++++++++++++++++++++++++++++++

Load metadata from JSON format into a ``pydicom.dataset.Dataset`` object.
A common use for this is translating metadata received from a ``RetrieveMetadata`` or a ``SearchFor``-style request:

.. code-block:: python

    from dicomweb_client.api import load_json_dataset

    metadata = client.retrieve_study_metadata('1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639')
    metadata_datasets = [load_json_dataset(ds) for ds in metadata]


Command Line Interface (CLI)
----------------------------

Search for studies:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/qidors search studies

Retrieve metadata for all instances of a given study:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/wadors \
        retrieve studies \
        --study 1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639 \
        metadata

The output can be *dicomized* for human interpretation:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/wadors \
        retrieve studies \
        --study 1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639 \
        metadata \
        --dicomize

Retrieve the full Part 3.10 files for all instances of a given study:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/wadors \
        retrieve studies \
        --study 1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639 \
        full


Retrieve a single frame of a given instances as JPEG compressed image:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/wadors \
        retrieve instances \
        --study 1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639 \
        --series 1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034 \
        --instance 1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534 \
        frames \
        --numbers 1 \
        --media-type image/jpeg

Store instances to a Google DICOMweb store:

.. code-block:: none

    dicomweb_client --url https://healthcare.googleapis.com/v1beta1/projects/MYPROJECT/locations/us-central1/datasets/MYDATASET/dicomStores/MYDICOMSTORE/dicomWeb \
        --token $(gcloud auth print-access-token) \
        store instances \
        dicomfiles/*
