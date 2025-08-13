import os
import sys
from collections.abc import Generator
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

AIRFLOW_HOME = Path.cwd() / "tests" / "airflow"
AIRFLOW_HOME.mkdir(parents=True, exist_ok=True)
os.environ["AIRFLOW_HOME"] = str(AIRFLOW_HOME)
os.environ["AIRFLOW__CORE__UNIT_TEST_MODE"] = "True"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS"] = "False"

from airflow.exceptions import AirflowNotFoundException  # noqa: E402
from airflow.models import DagBag  # noqa: E402
from airflow.utils.db import resetdb  # noqa: E402

from common.hooks.duckdb import DuckDBHook  # noqa: E402


class MockConnection:
    """Mock connection class for testing."""

    def __init__(self, conn_id, extra_dejson=None):
        self.conn_id = conn_id
        self.extra_dejson = extra_dejson or {}


@pytest.fixture(scope="session", autouse=True)
def reset_db():
    resetdb()


@pytest.fixture(scope="class")
def dagbag() -> DagBag:
    """Fixture to provide DagBag for integration tests.

    Returns:
        An instance of Airflow's DagBag for integration tests.
    """
    return DagBag(dag_folder="airflow/dags", include_examples=False)


@pytest.fixture
def mock_airflow_connections() -> Generator[dict, None, None]:
    """Mock Airflow connections.

    Yields:
        A dictionary of mocked Airflow connections.
    """
    connections = {
        "raw_dir": MockConnection(conn_id="raw_dir", extra_dejson={"path": "/tmp/raw"}),
        "warehouse_dir": MockConnection(
            conn_id="warehouse_dir",
            extra_dejson={"path": "/tmp/warehouse"},
        ),
        "raw_arxiv_json": MockConnection(
            conn_id="raw_arxiv_json",
            extra_dejson={"path": "/tmp/raw/arxiv.json"},
        ),
        "publications_parquet": MockConnection(
            conn_id="publications_parquet",
            extra_dejson={"path": "/tmp/publications.parquet"},
        ),
        "authors_parquet": MockConnection(
            conn_id="authors_parquet",
            extra_dejson={"path": "/tmp/authors.parquet"},
        ),
        "publications_authors_parquet": MockConnection(
            conn_id="publications_authors_parquet",
            extra_dejson={"path": "/tmp/publications_authors.parquet"},
        ),
        "duckdb_default": MockConnection(
            conn_id="duckdb_default",
            extra_dejson={"path": ":memory:"},
        ),
    }

    with patch(
        "airflow.models.connection.Connection.get_connection_from_secrets",
    ) as mock_get:

        def get_connection_side_effect(conn_id: str):
            if conn_id in connections:
                return connections[conn_id]

            msg = f"The conn_id `{conn_id}` isn't defined"
            raise AirflowNotFoundException(msg)

        mock_get.side_effect = get_connection_side_effect
        yield connections


@pytest.fixture
def mock_duckdb_hook() -> Generator[DuckDBHook, None, None]:
    """Mock DuckDB hook for testing.

    Yields:
        A mocked DuckDBHook instance for testing.
    """
    with patch("airflow.hooks.base.BaseHook.get_connection") as mock_get_conn:
        mock_connection = Mock()
        mock_connection.extra_dejson = {"path": "/tmp/test.db"}
        mock_get_conn.return_value = mock_connection

        # Add the dags directory to path for import
        dags_path = "/workspaces/academic-scope-demo/airflow/dags"
        if dags_path not in sys.path:
            sys.path.insert(0, dags_path)

        hook = DuckDBHook()
        yield hook


@pytest.fixture
def mock_task_instance() -> Mock:
    """Mock TaskInstance for testing.

    Returns:
        A mocked TaskInstance object for testing.
    """
    mock_ti = Mock()
    mock_ti.task_id = "test_task"
    mock_ti.dag_id = "test_dag"
    mock_ti.run_id = "test_run"
    mock_ti.log = Mock()
    return mock_ti
