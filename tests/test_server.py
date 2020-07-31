import pytest
import httpx

def test_auth():
    c = httpx.Client(headers={'auth':'abc123'})

    r = c.get('http://localhost:8765/health')
    assert r.status_code == 200

    r = httpx.get('http://localhost:8765/health')
    assert r.status_code == 401
