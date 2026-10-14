"""Focused checks for filenames derived from server-provided UIDs."""
import os
from unittest import mock

import pytest

from dicomweb_client import cli


@pytest.mark.parametrize('save_kind, extension', [
    ('instance', '.dcm'), ('metadata', '.json')
])
@pytest.mark.parametrize('uid_kind', ['normal', 'parent', 'absolute'])
def test_save_output_path(tmp_path, save_kind, extension, uid_kind):
    if uid_kind == 'normal':
        uid = '1.2.3'
    elif uid_kind == 'parent':
        uid = '../escaped'
    else:
        uid = str(tmp_path.parent / 'escaped')
    saver = (cli._save_instance if save_kind == 'instance'
             else cli._save_metadata)
    with mock.patch.object(cli.pydicom, 'dcmwrite') as dcmwrite:
        with mock.patch('builtins.open', mock.mock_open()) as open_file:
            if uid_kind == 'normal':
                saver({}, str(tmp_path), uid)
                expected = os.path.join(str(tmp_path), uid + extension)
                if save_kind == 'instance':
                    dcmwrite.assert_called_once_with(expected, {})
                    open_file.assert_not_called()
                else:
                    open_file.assert_called_once_with(expected, 'w')
                    dcmwrite.assert_not_called()
            else:
                with pytest.raises(ValueError, match='output directory'):
                    saver({}, str(tmp_path), uid)
                dcmwrite.assert_not_called()
                open_file.assert_not_called()
