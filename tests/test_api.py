from dicomweb_client.protocol import DICOMClient


def test_web_client_interface(client):
    assert isinstance(client, DICOMClient)


def test_file_client_interface(file_client):
    assert isinstance(file_client, DICOMClient)
