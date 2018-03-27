.. _user-guide:

User guide
==========

The client can be used with any DICOMweb server, such as `dcm4che <http://www.dcm4che.org/>`_ or `orthanc <https://www.orthanc-server.com/static.php?page=dicomweb>`_.

.. _examples:

Examples
--------

For the examples below, we will use the publicly accessible `RESTful DICOM services provided by DICOMweb Cloud <https://dicomcloud.azurewebsites.net>`_.


Active Programming Interface (API)
++++++++++++++++++++++++++++++++++

Search for instances:

.. code-block:: python

    from dicomweb_client.api import DICOMWebClient

    qidors = DICOMWebClient(url="https://dicomcloud.azurewebsites.net/qidors")
    instances = qidors.search_for_instances()
    print(instances)


Retrieve metadata for all instances of a given study:

.. code-block:: python

    from dicomweb_client.api import DICOMWebClient

    wadors = DICOMWebClient(url="https://dicomcloud.azurewebsites.net/wadors")
    study_uid = '1.2.392.200036.9116.2.5.1.37.2427178992.1440120482.32628'
    study_metadata = wadors.retrieve_study_metadata(study_uid)
    print(study_metadata)


Command Line Interface (CLI)
++++++++++++++++++++++++++++

Search for instances:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/qidors search instances

Retrieve metadata for all instances of a given study:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/wadors retrieve studies --study 1.2.392.200036.9116.2.5.1.37.2427178992.1440120482.32628 metadata


The output can be *dicomized* for human interpretation:

.. code-block:: none

    dicomweb_client --url https://dicomcloud.azurewebsites.net/wadors retrieve studies --study 1.2.392.200036.9116.2.5.1.37.2427178992.1440120482.32628 metadata --dicomize
