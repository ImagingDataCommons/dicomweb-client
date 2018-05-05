.. _conformance-statement:

Conformance statement
=====================

*Metadata* resource representations are requested in JSON format according to the `DICOM JSON model <http://dicom.nema.org/medical/dicom/current/output/chtml/part18/chapter_F.html>`_ using ``application/dicom+json`` media type.

*Bulkdata* and *frame* resource representations are by default requested as uncompressed pixel values using ``application/octet-stream``, but can alternatively be requested in JPEG, JPEG-LS or JPEG2000 compressed image format using ``image/jpeg``, ``image/x-jls`` or ``image/jpeg2000`` media type, respectively.

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
| GET    | RetrieveMetadata                              |       Y       |
+--------+-----------------------------------------------+---------------+
| GET    | RetrieveBulkdata                              |       Y\*     |
+--------+-----------------------------------------------+---------------+
| GET    | RetrieveFrames                                |       Y       |
+--------+-----------------------------------------------+---------------+
| GET    | RetrieveRenderedTransaction                   |       N       |
+--------+-----------------------------------------------+---------------+

\* not all options for retrieving rendered resource representations are implemented

STOW-RS
-------

+--------+-----------------------------------------------+---------------+
| Method | Resource                                      | Implemented   |
+========+===============================================+===============+
| POST   | StoreInstances                                |       Y       |
+--------+-----------------------------------------------+---------------+

