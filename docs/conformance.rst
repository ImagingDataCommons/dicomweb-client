.. _conformance-statement:

Conformance statement
=====================

QIDO-RS
-------

+--------+-----------------------------------------------+---------------+
| Method | Resource                                      | Implemented   |
+========+===============================================+===============+
| GET    | SearchForStudies                              |       Y       |
+--------+-----------------------------------------------+---------------+
| GET    | SearchForSeries                               |       Y       |
+--------+-----------------------------------------------+---------------+
| GET    | SearchForInstances                            |       Y       |
+--------+-----------------------------------------------+---------------+


WADO-RS
-------

+--------+-----------------------------------------------+---------------+
| Method | Resource                                      | Implemented   |
+========+===============================================+===============+
| GET    | RetrieveStudy                                 |       Y       |
+--------+-----------------------------------------------+---------------+
| GET    | RetrieveSeries                                |       Y       |
+--------+-----------------------------------------------+---------------+
| GET    | RetrieveInstance                              |       Y       |
+--------+-----------------------------------------------+---------------+
| GET    | RetrieveMetadata                              |       Y*      |
+--------+-----------------------------------------------+---------------+
| GET    | RetrieveBulkdata                              |       Y       |
+--------+-----------------------------------------------+---------------+
| GET    | RetrieveFrames                                |       Y       |
+--------+-----------------------------------------------+---------------+
| GET    | RetrieveRenderedTransaction                   |       Y       |
+--------+-----------------------------------------------+---------------+

* *Metadata* resource representations are requested in JSON format according to the `DICOM JSON model <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/chapter_F.html>`_ using ``application/dicom+json`` media type. Retrieval of metadata in XML form using ``application/dicom+xml`` is not supported.

STOW-RS
-------

+--------+-----------------------------------------------+---------------+
| Method | Resource                                      | Implemented   |
+========+===============================================+===============+
| POST   | StoreInstances                                |       Y       |
+--------+-----------------------------------------------+---------------+

