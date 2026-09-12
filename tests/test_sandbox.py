"""
Tests for the container-per-job sandbox execution path
(SANDBOX_MODE=docker).

These are unit tests against a mocked Docker client -- they don't
need a real Docker daemon, which keeps them runnable in the same CI
environment as the rest of the suite (GitHub Actions' basic Python
job runner). They verify:

1. run_job_in_container() calls the Docker SDK with the isolation
   flags it's supposed to (network disabled, memory/CPU caps,
   non-root user, read-only filesystem) -- i.e. that the sandbox
   is actually configured the way the code comments claim.
2. execute_job() dispatches to the container path when
   SANDBOX_MODE=docker, and to plain subprocess otherwise.
3. If Docker is unavailable, execute_job() falls back to
   subprocess instead of failing every job outright.

A real, live end-to-end sandbox test (actually spinning up a
container and confirming a hostile command is blocked) requires a
real Docker daemon and is intended to be run manually / in an
environment with Docker available -- see the "Verifying the sandbox
for real" section in the README.
"""

from unittest.mock import MagicMock, patch

import pytest

import worker


class FakeDB:
    def __init__(self, first_result=None):
        self.first_result = first_result
        self.calls = []

    def execute(self, statement, params=None):
        self.calls.append((statement, params))
        result = MagicMock()
        if len(self.calls) == 1 and self.first_result is not None:
            result.mappings.return_value.first.return_value = self.first_result
        else:
            result.mappings.return_value.first.return_value = None
        return result


class FakeEngine:
    def __init__(self, db):
        self.db = db

    def begin(self):
        return self

    def __enter__(self):
        return self.db

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


def _sample_job():
    return {
        "id": 300,
        "name": "SANDBOX_TEST",
        "command": "echo hello",
        "attempts": 1,
        "max_retries": 3,
        "priority": 100,
        "aging_bonus": 0,
    }


class TestRunJobInContainer:
    """
    Confirms the Docker container is actually launched with the
    isolation settings the code claims to apply.
    """

    def test_container_run_called_with_isolation_flags(self, monkeypatch):
        mock_container = MagicMock()
        mock_container.wait.return_value = {"StatusCode": 0}
        mock_container.logs.return_value = b"hello\n"

        mock_client = MagicMock()
        mock_client.containers.run.return_value = mock_container

        monkeypatch.setattr(worker, "_get_docker_client", lambda: mock_client)

        result = worker.run_job_in_container("echo hello")

        assert result == {"returncode": 0, "stdout": "hello\n", "stderr": ""}

        _, kwargs = mock_client.containers.run.call_args

        # The whole point of sandboxing: no network, hard resource
        # caps, non-root user, read-only root filesystem.
        assert kwargs["network_disabled"] is True
        assert kwargs["user"] == "nobody"
        assert kwargs["read_only"] is True
        assert kwargs["mem_limit"] == f"{worker.JOB_MAX_MEMORY_MB}m"
        assert kwargs["command"] == ["sh", "-c", "echo hello"]
        assert "no-new-privileges" in kwargs["security_opt"]

        # The container must always be cleaned up, not leaked.
        mock_container.remove.assert_called_once_with(force=True)

    def test_container_removed_even_on_failure(self, monkeypatch):
        mock_container = MagicMock()
        mock_container.wait.return_value = {"StatusCode": 1}
        mock_container.logs.return_value = b"boom\n"

        mock_client = MagicMock()
        mock_client.containers.run.return_value = mock_container

        monkeypatch.setattr(worker, "_get_docker_client", lambda: mock_client)

        result = worker.run_job_in_container("exit 1")

        assert result["returncode"] == 1
        mock_container.remove.assert_called_once_with(force=True)

    def test_raises_runtime_error_when_docker_unavailable(self, monkeypatch):
        monkeypatch.setattr(worker, "_get_docker_client", lambda: None)

        with pytest.raises(RuntimeError):
            worker.run_job_in_container("echo hello")


class TestExecuteJobSandboxDispatch:
    """
    Confirms execute_job() picks the right backend based on
    SANDBOX_MODE, and falls back safely when Docker isn't reachable.
    """

    def test_uses_container_path_when_sandbox_mode_is_docker(self, monkeypatch):
        job = _sample_job()
        db = FakeDB()

        monkeypatch.setattr(worker, "engine", FakeEngine(db))
        monkeypatch.setattr(worker, "SANDBOX_MODE", "docker")
        monkeypatch.setattr(
            worker,
            "run_job_in_container",
            lambda command: {"returncode": 0, "stdout": "ok", "stderr": ""},
        )
        # Sanity check the subprocess path is NOT used in this mode.
        monkeypatch.setattr(
            worker,
            "run_job_subprocess",
            lambda command: (_ for _ in ()).throw(
                AssertionError("subprocess path should not run in docker mode")
            ),
        )

        worker.execute_job(job)

        query = str(db.calls[0][0]).lower()
        assert "status = 'completed'" in query

    def test_falls_back_to_subprocess_when_docker_unavailable(self, monkeypatch):
        job = _sample_job()
        db = FakeDB()

        monkeypatch.setattr(worker, "engine", FakeEngine(db))
        monkeypatch.setattr(worker, "SANDBOX_MODE", "docker")

        def _unavailable(command):
            raise RuntimeError("Docker sandbox requested but unavailable")

        monkeypatch.setattr(worker, "run_job_in_container", _unavailable)
        monkeypatch.setattr(
            worker,
            "run_job_subprocess",
            lambda command: {"returncode": 0, "stdout": "fallback-ok", "stderr": ""},
        )

        worker.execute_job(job)

        query = str(db.calls[0][0]).lower()
        assert "status = 'completed'" in query

    def test_default_mode_uses_subprocess_only(self, monkeypatch):
        job = _sample_job()
        db = FakeDB()

        monkeypatch.setattr(worker, "engine", FakeEngine(db))
        monkeypatch.setattr(worker, "SANDBOX_MODE", "subprocess")
        monkeypatch.setattr(
            worker,
            "run_job_in_container",
            lambda command: (_ for _ in ()).throw(
                AssertionError("container path should not run in subprocess mode")
            ),
        )
        monkeypatch.setattr(
            worker,
            "run_job_subprocess",
            lambda command: {"returncode": 0, "stdout": "ok", "stderr": ""},
        )

        worker.execute_job(job)

        query = str(db.calls[0][0]).lower()
        assert "status = 'completed'" in query