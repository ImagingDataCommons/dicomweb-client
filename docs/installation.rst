.. _installation-guide:

Installation guide
==================

.. _requirements:

Requirements
------------

* `Python <https://www.python.org/>`_ (version 3.5 or higher)
* Python package manager `pip <https://pip.pypa.io/en/stable/>`_

For support of image formats:

* JPEG (`libjpeg <http://ijg.org/>`_ or `libjpeg-turbo <https://www.libjpeg-turbo.org/>`_)
* JPEG2000 (`openjpeg <http://www.openjpeg.org/>`_)
* PNG (`libpng <http://libpng.org/pub/png/libpng.html>`_)

.. _installation:

Installation
------------

Pre-build package available at PyPi:

.. code-block:: none

    pip install dicomweb-client

Additional dependencies required for extensions compatible with
`Google Cloud Platform (GCP)`_ may be installed as:

.. _Google Cloud Platform (GCP): https://cloud.google.com

.. code-block:: none

    pip install dicomweb-client[gcp]

Source code available at Github:

.. code-block:: none

    git clone https://github.com/mghcomputationalpathology/dicomweb-client ~/dicomweb-client
    pip install ~/dicomweb-client
