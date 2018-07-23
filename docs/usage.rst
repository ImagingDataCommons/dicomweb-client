.. _user-guide:

User guide
==========

The client can be used with any DICOMweb server, such as `dcm4che <http://www.dcm4che.org/>`_ or `orthanc <https://www.orthanc-server.com/static.php?page=dicomweb>`_.

.. _examples:

Examples
--------

For the examples below, we will use the publicly accessible `RESTful services provided by DICOMweb Cloud <https://dicomcloud.azurewebsites.net>`_ (note that *DICOMweb Cloud* uses different URL prefixes for the RESTful services QIDO-RS, WADO-RS and STOW-RS).


Active Programming Interface (API)
++++++++++++++++++++++++++++++++++

Instantiate the client:

.. code-block:: python

    from dicomweb_client.api import DICOMwebClient

    client = DICOMwebClient(
        url="https://dicomcloud.azurewebsites.net",
        qido_url_prefix="qidors", wado_url_prefix="wadors",
        stow_url_prefix="stowrs"
    )

Search for instances:

.. code-block:: python

    instances = client.search_for_instances()
    print(instances)


Retrieve metadata for all instances of a given study:

.. code-block:: python

    study_instance_uid = '1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639'
    study_metadata = client.retrieve_study_metadata(study_instance_uid)
    print(study_metadata)


Retrieve a single frame of a given instances as JPEG compressed image and show it:

.. code-block:: python

    from PIL import Image
    from io import BytesIO

    study_instance_uid = '1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639'
    series_instance_uid = '1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034'
    sop_instance_uid = '1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534'
    frames = client.retrieve_instance_frames(
        study_instance_uid, series_instance_uid, sop_instance_uid,
        frame_numbers=[1], image_format='jpeg'
    )

    image = Image.open(BytesIO(frames[0]))
    image.show()


Command Line Interface (CLI)
++++++++++++++++++++++++++++

Search for instances:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/qidors search instances

Retrieve metadata for all instances of a given study:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/wadors retrieve studies --study 1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639 metadata


The output can be *dicomized* for human interpretation:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/wadors retrieve studies --study 1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639 metadata --dicomize

Retrieve a single frame of a given instances as JPEG compressed image and show it:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/wadors retrieve instances --study 1.2.826.0.1.3680043.8.1055.1.20111103111148288.98361414.79379639 --series 1.2.826.0.1.3680043.8.1055.1.20111103111208937.49685336.24517034 --instance 1.2.826.0.1.3680043.8.1055.1.20111103111208937.40440871.13152534 frames --numbers 1 --image-format jpeg --show
