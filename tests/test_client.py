import pytest

from pysparkrpc.api_client import PROXY_URL

def test_configurable_url():
    assert PROXY_URL == 'http://127.0.0.1:8765'
