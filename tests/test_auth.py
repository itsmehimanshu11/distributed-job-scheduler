import os

from app.main import verify_api_key


def test_valid_api_key(monkeypatch):
    """A correct API key should be accepted."""

    monkeypatch.setenv("API_KEY", "test-secret-key")

    result = verify_api_key("test-secret-key")

    assert result is True


def test_invalid_api_key(monkeypatch):
    """An incorrect API key should be rejected."""

    from fastapi import HTTPException

    monkeypatch.setenv("API_KEY", "test-secret-key")

    try:
        verify_api_key("wrong-key")
        assert False, "Expected HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 401
        assert exc.detail == "Invalid or missing API key"


def test_missing_api_key(monkeypatch):
    """A missing API key should be rejected."""

    from fastapi import HTTPException

    monkeypatch.setenv("API_KEY", "test-secret-key")

    try:
        verify_api_key(None)
        assert False, "Expected HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 401
        assert exc.detail == "Invalid or missing API key"


def test_api_key_not_configured(monkeypatch):
    """The API should report a server error when API_KEY is not configured."""

    from fastapi import HTTPException

    monkeypatch.delenv("API_KEY", raising=False)

    try:
        verify_api_key("anything")
        assert False, "Expected HTTPException"
    except HTTPException as exc:
        assert exc.status_code == 500
        assert exc.detail == "API_KEY is not configured"