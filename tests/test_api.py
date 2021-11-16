from dicomweb_client.protocol import DICOMwebProtocol


def test_web_client_interface(client):
    assert isinstance(client, DICOMwebProtocol)


def test_file_client_interface(file_client):
    assert isinstance(file_client, DICOMwebProtocol)
