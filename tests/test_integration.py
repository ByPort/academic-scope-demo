from unittest.mock import Mock, patch

import pytest

from tests.conftest import MockConnection


def create_execution_context() -> dict:
    """Create a proper Airflow execution context for tests.

    Returns:
        A dictionary representing the Airflow execution context for tests.
    """
    ti_mock = Mock()
    ti_mock.render_templates = Mock()

    return {
        "dag": Mock(),
        "ds": "2023-01-01",
        "ts": "2023-01-01T00:00:00+00:00",
        "ti": ti_mock,
        "task": Mock(),
        "run_id": "test_run",
        "task_instance": ti_mock,
        "execution_date": "2023-01-01",
        "dag_run": Mock(),
        "conf": {},
        "params": {},
        "var": {"value": Mock(), "json": Mock()},
        "task_instance_key_str": "test_task_instance",
    }


class TestDAGStructure:
    """Test DAG structure and configuration without execution."""

    def test_dag_loads_successfully(self, dagbag):
        """Test that the DAG can be loaded without errors."""
        dag = dagbag.get_dag(dag_id="arxiv_etl")
        assert dag is not None
        assert dag.dag_id == "arxiv_etl"
        assert len(dag.tasks) == 13  # Exact number to catch missing/extra tasks

    def test_task_dependencies(self, dagbag):
        """Test that task dependencies are correctly configured."""
        dag = dagbag.get_dag(dag_id="arxiv_etl")

        # Get key tasks
        check_task = dag.get_task("check_if_zip_exists")
        extract_pubs = dag.get_task("extract_publications")
        load_pubs = dag.get_task("load_publications")

        # Verify dependencies - This will catch dependency mistakes!
        extract_upstream = extract_pubs.upstream_task_ids
        load_upstream = load_pubs.upstream_task_ids

        assert "prepare_warehouse" in extract_upstream
        assert "extract_publications" in load_upstream

        # Check branching structure
        downstream_task_ids = check_task.downstream_task_ids
        assert "skip_download_zip" in downstream_task_ids
        assert "prepare_raw_folder" in downstream_task_ids


class TestBranchingLogic:
    """Test the branching logic with real validation."""

    @patch("airflow.sdk.definitions.connection.Connection.get")
    @patch("pathlib.Path.exists")
    def test_check_if_zip_exists_when_file_exists(
        self,
        mock_exists,
        mock_connection_get,
        dagbag,
    ):
        """Test branching when zip file exists - should return skip_download_zip."""
        # Setup
        mock_conn = Mock()
        mock_conn.extra_dejson = {"path": "/tmp/test.zip"}
        mock_connection_get.return_value = mock_conn
        mock_exists.return_value = True

        # Execute
        dag = dagbag.get_dag(dag_id="arxiv_etl")
        task = dag.get_task("check_if_zip_exists")
        result = task.python_callable(
            choices=("skip_download_zip", "prepare_raw_folder"),
        )

        # Validate - this will fail if choices are swapped!
        assert result == "skip_download_zip"
        # The Path() call in the function doesn't pass args to exists()
        mock_exists.assert_called_once()

    @patch("airflow.sdk.definitions.connection.Connection.get")
    @patch("pathlib.Path.exists")
    def test_check_if_zip_exists_when_file_missing(
        self,
        mock_exists,
        mock_connection_get,
        dagbag,
    ):
        """Test branching when zip file missing - should return prepare_raw_folder."""
        # Setup
        mock_conn = Mock()
        mock_conn.extra_dejson = {"path": "/tmp/test.zip"}
        mock_connection_get.return_value = mock_conn
        mock_exists.return_value = False

        # Execute
        dag = dagbag.get_dag(dag_id="arxiv_etl")
        task = dag.get_task("check_if_zip_exists")
        result = task.python_callable(
            choices=("skip_download_zip", "prepare_raw_folder"),
        )

        # Validate - this will fail if choices are swapped!
        assert result == "prepare_raw_folder"
        # The Path() call in the function doesn't pass args to exists()
        mock_exists.assert_called_once()


class TestSQLTemplateValidation:
    """Test that SQL templates are syntactically correct and use right connections."""

    def test_extract_publications_sql_syntax(self, dagbag):
        """
        Test that extract_publications returns valid SQL
        with correct connection references.
        """
        dag = dagbag.get_dag(dag_id="arxiv_etl")
        task = dag.get_task("extract_publications")

        # Get the SQL from the task function
        sql = task.python_callable()

        # Validate SQL structure
        assert "COPY" in sql.upper()
        assert "SELECT" in sql.upper()
        assert "FROM" in sql.upper()
        assert "TO" in sql.upper()

        # Validate connection references are correct
        assert "{{ conn.raw_arxiv_json.extra_dejson.path }}" in sql
        assert "{{ conn.publications_parquet.extra_dejson.path }}" in sql

        # Check for typos in column names - this will catch spelling mistakes!
        assert "id, title, abstract, categories, update_date, submitter" in sql

    def test_prepare_warehouse_sql_syntax(self, dagbag):
        """Test that prepare_warehouse returns valid SQL schema."""
        dag = dagbag.get_dag(dag_id="arxiv_etl")
        task = dag.get_task("prepare_warehouse")

        sql = task.python_callable()

        # Validate table creation statements
        assert "CREATE OR REPLACE TABLE publications" in sql
        assert "CREATE OR REPLACE TABLE authors" in sql
        assert "CREATE OR REPLACE TABLE publications_authors" in sql

        # Check for critical columns - this will catch schema mistakes!
        assert "id VARCHAR" in sql
        assert "title VARCHAR" in sql
        assert "name VARCHAR" in sql
        assert "publication_id VARCHAR" in sql
        assert "author_name VARCHAR" in sql


class TestBashCommandValidation:
    """Test that bash commands are properly configured."""

    def test_download_zip_command_structure(self, dagbag):
        """Test that download_zip has proper bash command."""
        dag = dagbag.get_dag(dag_id="arxiv_etl")
        task = dag.get_task("download_zip")

        # For decorated tasks, the command is in the task callable
        command = task.python_callable()

        # Should contain curl command - this will catch command typos!
        assert "curl" in command.lower()
        # Should reference the connection
        assert "conn.raw_arxiv_zip.extra_dejson.path" in command

    def test_unzip_command_structure(self, dagbag):
        """Test that unzip has proper bash command."""
        dag = dagbag.get_dag(dag_id="arxiv_etl")
        task = dag.get_task("unzip")

        # For decorated tasks, the command is in the task callable
        command = task.python_callable()

        # Should contain unzip command - this will catch command typos!
        assert "unzip" in command.lower()
        # Should reference connections
        assert "conn.raw_arxiv_zip.extra_dejson.path" in command


class TestDuckDBTaskExecution:
    """Test DuckDB task execution with minimal but effective mocking."""

    @patch("airflow.models.connection.Connection.get_connection_from_secrets")
    def test_prepare_warehouse_executes_correct_sql(self, mock_get_connection, dagbag):
        """Test that prepare_warehouse executes the expected SQL statements."""
        # Setup
        mock_get_connection.return_value = MockConnection(
            "duckdb_default",
            {"database": ":memory:"},
        )

        # Get task
        dag = dagbag.get_dag(dag_id="arxiv_etl")
        task = dag.get_task("prepare_warehouse")
        context = create_execution_context()

        # Mock DuckDB to capture what SQL is executed
        with patch("duckdb.connect") as mock_connect:
            mock_conn = Mock()
            executed_sql = []

            def capture_sql(sql):
                executed_sql.append(sql)

            mock_conn.sql.side_effect = capture_sql
            mock_connect.return_value.__enter__.return_value = mock_conn

            # Execute
            task.execute(context)

            # Validate that correct SQL was executed
            assert len(executed_sql) == 1
            sql = executed_sql[0]

            # Check all tables are created - this will catch missing tables!
            assert "CREATE OR REPLACE TABLE publications" in sql
            assert "CREATE OR REPLACE TABLE authors" in sql
            assert "CREATE OR REPLACE TABLE publications_authors" in sql


class TestConnectionUsage:
    """Test that tasks use the correct connections."""

    @patch("airflow.sdk.definitions.connection.Connection.get")
    def test_folder_tasks_use_correct_connections(self, mock_connection_get, dagbag):
        """Test that folder preparation tasks use the right connection IDs."""
        dag = dagbag.get_dag(dag_id="arxiv_etl")

        # Mock connection to track which connections are requested
        requested_connections = []

        def track_connection_calls(conn_id):
            requested_connections.append(conn_id)
            mock_conn = Mock()
            mock_conn.extra_dejson = {"path": f"/tmp/{conn_id}"}
            return mock_conn

        mock_connection_get.side_effect = track_connection_calls

        # Test prepare_raw_folder
        with patch("pathlib.Path.mkdir"):
            raw_task = dag.get_task("prepare_raw_folder")
            raw_task.python_callable()

        # Test prepare_warehouse_folder
        with patch("pathlib.Path.mkdir"):
            warehouse_task = dag.get_task("prepare_warehouse_folder")
            warehouse_task.python_callable()

        # Validate correct connections were used - this will catch connection ID typos!
        assert "raw_dir" in requested_connections
        assert "warehouse_dir" in requested_connections


class TestErrorScenarios:
    """Test error handling with real validation."""

    @patch("airflow.sdk.definitions.connection.Connection.get")
    def test_missing_connection_raises_error(self, mock_connection_get, dagbag):
        """Test that missing connections cause proper failures."""
        mock_connection_get.side_effect = KeyError(
            "Connection 'missing_conn' not found",
        )

        dag = dagbag.get_dag(dag_id="arxiv_etl")
        task = dag.get_task("check_if_zip_exists")

        # This should fail with the connection error
        with pytest.raises(KeyError, match=r"Connection.*not found"):
            task.python_callable(choices=("skip_download_zip", "prepare_raw_folder"))

    @patch("airflow.models.connection.Connection.get_connection_from_secrets")
    def test_duckdb_sql_error_propagates(self, mock_get_connection, dagbag):
        """Test that SQL errors are properly propagated."""
        # Setup connection
        mock_get_connection.return_value = MockConnection(
            "duckdb_default",
            {"database": ":memory:"},
        )

        # Get task
        dag = dagbag.get_dag(dag_id="arxiv_etl")
        task = dag.get_task("prepare_warehouse")
        context = create_execution_context()

        # Mock DuckDB to raise SQL error
        with patch("duckdb.connect") as mock_connect:
            mock_conn = Mock()
            mock_conn.sql.side_effect = Exception("SQL syntax error: invalid statement")
            mock_connect.return_value.__enter__.return_value = mock_conn

            # Should propagate the SQL error
            with pytest.raises(Exception, match="SQL syntax error"):
                task.execute(context)
