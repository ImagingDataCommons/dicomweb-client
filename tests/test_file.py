from pydicom.dataset import Dataset


STUDY_ATTRIBUTES = {
    'StudyInstanceUID',
    'StudyID',
    'StudyDate',
    'StudyTime',
    'PatientName',
    'PatientID',
    'PatientSex',
    'PatientBirthDate',
    'ModalitiesInStudy',
    'ReferringPhysicianName',
    'NumberOfStudyRelatedSeries',
    'NumberOfStudyRelatedInstances',
}


SERIES_ATTRIBUTES = {
    'Modality',
    'SeriesInstanceUID',
    'SeriesNumber',
    'NumberOfSeriesRelatedInstances'
}


INSTANCE_ATTRIBUTES = {
    'SOPClassUID',
    'SOPInstanceUID',
    'InstanceNumber',
    'TransferSyntaxUID',
}


def test_search_for_studies(file_client):
    studies = file_client.search_for_studies()
    assert isinstance(studies, list)
    assert len(studies) > 0

    for test_study_json in studies:
        assert isinstance(test_study_json, dict)
        test_study = Dataset.from_json(test_study_json)
        for attr in STUDY_ATTRIBUTES:
            assert hasattr(test_study, attr)


def test_search_for_studies_with_filters(file_client):
    studies = file_client.search_for_studies(
        search_filters={'PatientName': 'CQ500'},
        fuzzymatching=True
    )
    assert isinstance(studies, list)
    assert len(studies) > 0

    studies = file_client.search_for_studies(
        search_filters={'PatientName': 'CQ500*'},
    )
    assert isinstance(studies, list)
    assert len(studies) > 0

    studies = file_client.search_for_studies(
        search_filters={'PatientName': 'blahblah'}
    )
    assert isinstance(studies, list)
    assert len(studies) == 0


def test_search_for_studies_with_limit_and_offset(file_client):
    studies = file_client.search_for_studies(
        limit=2,
        offset=1
    )
    assert isinstance(studies, list)
    assert len(studies) == 2

    studies = file_client.search_for_studies(
        limit=1,
        offset=0
    )
    assert isinstance(studies, list)
    assert len(studies) == 1


def test_search_for_series(file_client):
    series = file_client.search_for_series()
    assert isinstance(series, list)
    assert len(series) > 0

    for test_series_json in series:
        assert isinstance(test_series_json, dict)
        test_series = Dataset.from_json(test_series_json)
        for attr in STUDY_ATTRIBUTES | SERIES_ATTRIBUTES:
            assert hasattr(test_series, attr)


def test_search_for_series_in_study(file_client):
    series = file_client.search_for_series(
        '1.2.276.0.7230010.3.1.2.296485376.1.1521713414.1800996'
    )
    assert isinstance(series, list)
    assert len(series) > 0

    for test_series_json in series:
        assert isinstance(test_series_json, dict)
        test_series = Dataset.from_json(test_series_json)
        for attr in SERIES_ATTRIBUTES:
            assert hasattr(test_series, attr)
        for attr in STUDY_ATTRIBUTES:
            assert not hasattr(test_series, attr)


def test_search_for_series_with_filter(file_client):
    series = file_client.search_for_series(
        search_filters={'Modality': 'CT'}
    )
    assert isinstance(series, list)
    assert len(series) > 0

    for test_series_json in series:
        assert isinstance(test_series_json, dict)
        test_series = Dataset.from_json(test_series_json)
        for attr in SERIES_ATTRIBUTES | STUDY_ATTRIBUTES:
            assert hasattr(test_series, attr)


def test_search_for_instances(file_client):
    instances = file_client.search_for_instances()
    assert isinstance(instances, list)
    assert len(instances) > 0

    for test_instance_json in instances:
        assert isinstance(test_instance_json, dict)
        test_instance_pydicom = Dataset.from_json(test_instance_json)
        for attr in INSTANCE_ATTRIBUTES | SERIES_ATTRIBUTES | STUDY_ATTRIBUTES:
            assert hasattr(test_instance_pydicom, attr)


def test_search_for_instances_in_study(file_client):
    instances = file_client.search_for_instances(
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.1'
    )
    assert isinstance(instances, list)
    assert len(instances) > 0

    for test_instance_json in instances:
        assert isinstance(test_instance_json, dict)
        test_instance_pydicom = Dataset.from_json(test_instance_json)
        for attr in INSTANCE_ATTRIBUTES:
            assert hasattr(test_instance_pydicom, attr)
        for attr in SERIES_ATTRIBUTES:
            assert hasattr(test_instance_pydicom, attr)
        for attr in STUDY_ATTRIBUTES:
            assert not hasattr(test_instance_pydicom, attr)


def test_search_for_instances_in_series(file_client):
    instances = file_client.search_for_instances(
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.1',
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.2'
    )
    assert isinstance(instances, list)
    assert len(instances) > 0

    for test_instance_json in instances:
        assert isinstance(test_instance_json, dict)
        test_instance_pydicom = Dataset.from_json(test_instance_json)
        for attr in INSTANCE_ATTRIBUTES:
            assert hasattr(test_instance_pydicom, attr)
        for attr in SERIES_ATTRIBUTES:
            assert not hasattr(test_instance_pydicom, attr)
        for attr in STUDY_ATTRIBUTES:
            assert not hasattr(test_instance_pydicom, attr)


def retrieve_series_metadata(file_client):
    instances = file_client.retrieve_series_metadata(
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.1',
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.2'
    )
    assert isinstance(instances, list)
    assert len(instances) > 0

    for test_instance_json in instances:
        assert isinstance(test_instance_json, dict)
        test_instance_pydicom = Dataset.from_json(test_instance_json)
        attributes = {
            'SOPClassUID',
            'SOPInstanceUID',
            'SeriesInstanceUID',
            'StudyInstanceUID',
        }
        for attr in attributes:
            assert hasattr(test_instance_pydicom, attr)


def retrieve_instance_metadata(file_client):
    instance = file_client.retrieve_instance_metadata(
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.1',
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.2',
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.95'
    )
    assert isinstance(instance, dict)

    instance_pydicom = Dataset.from_json(instance)
    attributes = {
        'SOPClassUID',
        'SOPInstanceUID',
        'SeriesInstanceUID',
        'StudyInstanceUID',
    }
    for attr in attributes:
        assert hasattr(instance_pydicom, attr)


def retrieve_instance(file_client):
    instance = file_client.retrieve_instance(
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.1',
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.2',
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.95'
    )
    assert isinstance(instance, Dataset)


def retrieve_instance_frames(file_client):
    frames = file_client.retrieve_instance_frames(
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.1',
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.2',
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.95',
        frame_numbers=[1]
    )
    assert isinstance(frames, list)
    assert len(frames) > 0

    for test_frame in frames:
        assert isinstance(test_frame, bytes)


def retrieve_instance_frames_rendered(file_client):
    frame = file_client.retrieve_instance_frames_rendered(
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.1',
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.2',
        '1.3.6.1.4.1.5962.1.1.0.0.0.1196530851.28319.0.95',
        frame_numbers=[1]
    )
    assert isinstance(frame, bytes)
