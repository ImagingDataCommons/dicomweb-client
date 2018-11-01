.. _user-guide:

User guide
==========

The client can be used with any DICOMweb server, such as `dcm4che <http://www.dcm4che.org/>`_, `orthanc <https://www.orthanc-server.com/static.php?page=dicomweb>`_ or `DICOMcloud <https://dicomcloud.github.io/>`_.

.. _api:

Active Programming Interface (API)
----------------------------------

To interact with a publicly accessible server, you only need to provide the ``url`` for the server address.

.. code-block:: python

    from dicomweb_client.api import DICOMwebClient

    client = DICOMwebClient("https://mydicomwebserver.com")


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


To interact with server requiring authentication, you can provide your credentials using the ``username`` and ``password`` arguments.

.. code-block:: python

    from dicomweb_client.api import DICOMwebClient

    client = DICOMwebClient(
        url="https://mydicomwebserver.com",
        username="myusername",
        password="mypassword"
    )


To interact with server requiring certificate-based authentication, you can provide the CA bundle and client certificate using the ``ca_bundle`` and ``cert`` arguments, respectively.

.. code-block:: python

    from dicomweb_client.api import DICOMwebClient

    client = DICOMwebClient(
        url="https://mydicomwebserver.com",
        ca_bundle="/path/to/ca.crt",
        cert="/path/to/cert.pem"
    )


.. _searchforstudies:

QIDO-RS SeachForStudies
+++++++++++++++++++++++

Search for all studies:

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

    studies = client.search_for_studies()
    batch_size = len(studies)
    count = 1
    while True:
        offset = batch_size * count
        try:
            subset = client.search_for_studies(offset=offset)
        except requests.exceptions.HTTPError:
            break
        studies.extend(subset)
        count += 1

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


.. _retrieveinstance:

WADO-RS RetrieveInstance
++++++++++++++++++++++++

Retrieve instance:

.. code-block:: python

    instance = client.retrieve_instance(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639',
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034'
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534'
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
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034'
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534'
    )

.. note::

    WADO-RS RetrieveMetadata always returns metadata at the instance-level, ``retrieve_study_metadata()`` and ``retrieve_series_metadata()`` return an array of metadata items for each instance belonging to a given study and series, respectively.


.. _retrieveframes:

WADO-RS RetrieveFrames
++++++++++++++++++++++


Retrieve a set of frames of a given instances as JPEG compressed image:

.. code-block:: python

    frames = client.retrieve_instance_frames(
        study_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639'
        series_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034'
        sop_instance_uid='1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534'
        frame_numbers=[1, 2],
        image_format='jpeg'
    )


Frames are returned as bytes. To convert the image into a *NumPy* array you can use the *PIL* module:

.. code-block:: python

    from io import BytesIO

    import numpy as np
    from PIL import Image

    image = Image.open(BytesIO(frames[0]))
    array = np.array(image)


.. _retrievebulkdata:

WADO-RS RetrieveBulkdata
++++++++++++++++++++++++

Retrieve bulk data given a URL:

.. code-block:: python

    data = client.retrieve_bulkdata('https://mydicomwebserver.com/studies/...')


.. _cli:

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
        --image-format jpeg

