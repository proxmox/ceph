from copy import deepcopy

import pytest

from rook_client.ceph import cephcluster as cc


def test_omit():
    cv = cc.CephVersion()
    with pytest.raises(AttributeError):
        cv.allowUnsupported

    assert not hasattr(cv, 'allowUnsupported')
