import logging
from unittest.mock import MagicMock, patch

import pytest
from airflow.exceptions import AirflowNotFoundException

from common.hooks.duckdb import DuckDBHook


class TestDuckDBHook:
    """Test class for DuckDBHook."""

    def test_hook_initialization_default(self, mock_airflow_connections):
        """Test DuckDBHook initialization with default parameters."""
        hook = DuckDBHook()
        assert hook.duckdb_conn_id == "duckdb_default"
        assert hook.database == ":memory:"
        assert hook.config is None

    def test_hook_initialization_custom(self, mock_airflow_connections):
        """Test DuckDBHook initialization with custom parameters."""
        custom_config = {"memory_limit": "2GB", "threads": "4"}
        hook = DuckDBHook(duckdb_conn_id="duckdb_default", config=custom_config)
        assert hook.duckdb_conn_id == "duckdb_default"
        assert hook.config == custom_config

    def test_get_conn(self, mock_airflow_connections):
        """Test get_conn method."""
        hook = DuckDBHook()
        conn = hook.get_conn()
        assert conn.conn_id == "duckdb_default"
        assert conn.extra_dejson.get("path") == ":memory:"

    @patch("duckdb.connect")
    def test_sql_execution(self, mock_duckdb_connect, mock_airflow_connections):
        """Test SQL execution through the hook."""
        # Mock DuckDB connection that supports context manager
        mock_conn_instance = MagicMock()
        mock_duckdb_connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn_instance,
        )
        mock_duckdb_connect.return_value.__exit__ = MagicMock(return_value=False)

        hook = DuckDBHook()
        sql = "SELECT 1 as test_col"
        hook.sql(sql)

        # Verify DuckDB was called with correct parameters
        mock_duckdb_connect.assert_called_once_with(database=":memory:", config=None)
        mock_conn_instance.sql.assert_called_once_with(sql)

    @patch("duckdb.connect")
    def test_sql_execution_with_config(
        self,
        mock_duckdb_connect,
        mock_airflow_connections,
    ):
        """Test SQL execution with custom DuckDB config."""
        config = {"memory_limit": "1GB", "threads": "2"}
        # Mock DuckDB connection that supports context manager
        mock_conn_instance = MagicMock()
        mock_duckdb_connect.return_value.__enter__ = MagicMock(
            return_value=mock_conn_instance,
        )
        mock_duckdb_connect.return_value.__exit__ = MagicMock(return_value=False)

        hook = DuckDBHook(config=config)
        sql = "CREATE TABLE test (id INTEGER)"
        hook.sql(sql)

        # Verify DuckDB was called with custom config
        mock_duckdb_connect.assert_called_once_with(database=":memory:", config=config)
        mock_conn_instance.sql.assert_called_once_with(sql)

    @patch("duckdb.connect")
    def test_sql_execution_exception_handling(
        self,
        mock_duckdb_connect,
        mock_airflow_connections,
    ):
        """Test SQL execution exceptions are properly handled."""
        mock_duckdb_connect.side_effect = Exception("Database connection failed")

        hook = DuckDBHook()

        with pytest.raises(Exception, match="Database connection failed"):
            hook.sql("SELECT 1")

    def test_connection_error_handling(self, mock_airflow_connections):
        """Test that connection errors are properly handled."""
        with pytest.raises(AirflowNotFoundException):
            DuckDBHook(duckdb_conn_id="nonexistent_conn")

    def test_hook_logging(self, mock_airflow_connections, caplog):
        """Test that hook initialization logs are working."""
        with caplog.at_level(logging.INFO):
            _ = DuckDBHook(duckdb_conn_id="duckdb_default", config={"test": "value"})

        assert (
            "Initializing DuckDBHook with connection ID duckdb_default" in caplog.text
        )
