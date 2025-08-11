import os
import pytest
from unittest.mock import Mock, patch


AIRFLOW_HOME = os.path.join(os.getcwd(), "tests", "airflow")
os.makedirs(AIRFLOW_HOME, exist_ok=True)
os.environ["AIRFLOW_HOME"] = AIRFLOW_HOME
os.environ['AIRFLOW__CORE__UNIT_TEST_MODE'] = 'True'
os.environ['AIRFLOW__CORE__LOAD_EXAMPLES'] = 'False'
os.environ['AIRFLOW__CORE__LOAD_DEFAULT_CONNECTIONS'] = 'False'


class MockConnection:
    """Mock connection class for testing."""
    
    def __init__(self, conn_id, extra_dejson=None):
        self.conn_id = conn_id
        self.extra_dejson = extra_dejson or {}


@pytest.fixture(scope="session", autouse=True)
def resetdb():
    from airflow.utils.db import resetdb
    resetdb()


@pytest.fixture(scope="class")
def dagbag():
    """Fixture to provide DagBag for integration tests."""
    from airflow.models import DagBag
    return DagBag(dag_folder="airflow/dags", include_examples=False)


@pytest.fixture
def mock_airflow_connections():
    """Mock Airflow connections."""
    connections = {
        'raw_dir': MockConnection(
            conn_id='raw_dir',
            extra_dejson={'path': '/tmp/raw'}
        ),
        'warehouse_dir': MockConnection(
            conn_id='warehouse_dir', 
            extra_dejson={'path': '/tmp/warehouse'}
        ),
        'raw_arxiv_json': MockConnection(
            conn_id='raw_arxiv_json',
            extra_dejson={'path': '/tmp/raw/arxiv.json'}
        ),
        'publications_parquet': MockConnection(
            conn_id='publications_parquet',
            extra_dejson={'path': '/tmp/publications.parquet'}
        ),
        'authors_parquet': MockConnection(
            conn_id='authors_parquet',
            extra_dejson={'path': '/tmp/authors.parquet'}
        ),
        'publications_authors_parquet': MockConnection(
            conn_id='publications_authors_parquet',
            extra_dejson={'path': '/tmp/publications_authors.parquet'}
        ),
        'duckdb_default': MockConnection(
            conn_id='duckdb_default',
            extra_dejson={'path': ':memory:'}
        )
    }
    
    with patch('airflow.models.connection.Connection.get_connection_from_secrets') as mock_get:
        def get_connection_side_effect(conn_id):
            if conn_id in connections:
                return connections[conn_id]
            from airflow.exceptions import AirflowNotFoundException
            raise AirflowNotFoundException(f"The conn_id `{conn_id}` isn't defined")
        
        mock_get.side_effect = get_connection_side_effect
        yield connections


@pytest.fixture
def mock_duckdb_hook():
    """Mock DuckDB hook for testing."""
    with patch('airflow.hooks.base.BaseHook.get_connection') as mock_get_conn:
        mock_connection = Mock()
        mock_connection.extra_dejson = {'path': '/tmp/test.db'}
        mock_get_conn.return_value = mock_connection
        
        # Add the dags directory to path for import
        import sys
        dags_path = '/workspaces/academic-scope-demo/airflow/dags'
        if dags_path not in sys.path:
            sys.path.insert(0, dags_path)
        
        from common.hooks.duckdb import DuckDBHook
        hook = DuckDBHook()
        yield hook


@pytest.fixture
def mock_task_instance():
    """Mock TaskInstance for testing."""
    mock_ti = Mock()
    mock_ti.task_id = 'test_task'
    mock_ti.dag_id = 'test_dag'
    mock_ti.run_id = 'test_run'
    mock_ti.log = Mock()
    return mock_ti
