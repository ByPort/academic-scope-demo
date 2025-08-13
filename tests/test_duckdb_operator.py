from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from airflow.sdk import DAG

from common.operators.duckdb import DuckDBDecoratedOperator, DuckDBOperator, duckdb_task


class TestDuckDBOperator:
    """Test the DuckDBOperator class."""

    def test_operator_initialization(self):
        """Test operator initialization with required parameters."""
        operator = DuckDBOperator(task_id="test_task", sql="SELECT 1")

        assert operator.task_id == "test_task"
        assert operator.sql == "SELECT 1"
        assert operator.duckdb_conn_id == "duckdb_default"
        assert operator.config is None

    def test_operator_initialization_custom(self):
        """Test operator initialization with custom parameters."""
        config = {"memory_limit": "2GB"}
        operator = DuckDBOperator(
            task_id="test_task",
            sql="CREATE TABLE test (id INT)",
            duckdb_conn_id="custom_conn",
            config=config,
        )

        assert operator.task_id == "test_task"
        assert operator.sql == "CREATE TABLE test (id INT)"
        assert operator.duckdb_conn_id == "custom_conn"
        assert operator.config == config

    def test_template_fields(self):
        """Test that SQL is properly templated."""
        operator = DuckDBOperator(task_id="test_task", sql="SELECT 1")

        assert "sql" in operator.template_fields
        assert operator.template_fields_renderers["sql"] == "sql"

    @patch("common.operators.duckdb.DuckDBHook")
    def test_execute_success(self, mock_hook_class, mock_airflow_connections):
        """Test successful execution of the operator."""
        # Setup
        mock_hook = MagicMock()
        mock_result = MagicMock()
        mock_hook.sql.return_value = mock_result
        mock_hook_class.return_value = mock_hook

        operator = DuckDBOperator(task_id="test_task", sql="SELECT 1")

        # Execute
        result = operator.execute({})

        # Assertions
        mock_hook_class.assert_called_once_with(
            duckdb_conn_id="duckdb_default",
            config=None,
        )
        mock_hook.sql.assert_called_once_with("SELECT 1")
        assert result == mock_result

    @patch("common.operators.duckdb.DuckDBHook")
    def test_execute_with_config(self, mock_hook_class, mock_airflow_connections):
        """Test execution with custom configuration."""
        # Setup
        mock_hook = MagicMock()
        mock_result = MagicMock()
        mock_hook.sql.return_value = mock_result
        mock_hook_class.return_value = mock_hook

        config = {"memory_limit": "1GB", "threads": "2"}
        operator = DuckDBOperator(
            task_id="test_task",
            sql="CREATE TABLE test (id INT)",
            duckdb_conn_id="custom_conn",
            config=config,
        )

        # Execute
        result = operator.execute({})

        # Assertions
        mock_hook_class.assert_called_once_with(
            duckdb_conn_id="custom_conn",
            config=config,
        )
        mock_hook.sql.assert_called_once_with("CREATE TABLE test (id INT)")
        assert result == mock_result

    @patch("common.operators.duckdb.DuckDBHook")
    def test_execute_exception_handling(
        self,
        mock_hook_class,
        mock_airflow_connections,
    ):
        """Test that execution exceptions are properly handled."""
        # Setup
        mock_hook = MagicMock()
        mock_hook.sql.side_effect = Exception("SQL execution failed")
        mock_hook_class.return_value = mock_hook

        operator = DuckDBOperator(task_id="test_task", sql="INVALID SQL")

        # Test
        with pytest.raises(Exception, match="SQL execution failed"):
            operator.execute({})

    def test_sql_templating(self, mock_airflow_connections):
        """Test SQL templating with Airflow variables."""
        with DAG(dag_id="test_dag", start_date=datetime(2023, 1, 1)) as dag:
            operator = DuckDBOperator(
                task_id="test_task",
                sql="SELECT * FROM {{ conn.duckdb_default.extra_dejson.path }}",
                dag=dag,
            )

        # Check that the SQL contains template syntax
        assert "{{" in operator.sql
        assert "conn.duckdb_default" in operator.sql


class TestDuckDBDecoratedOperator:
    """Test the DuckDBDecoratedOperator class."""

    def test_decorated_operator_attributes(self):
        """Test decorated operator class attributes."""
        assert DuckDBDecoratedOperator.custom_operator_name == "@duckdb_task"
        assert DuckDBDecoratedOperator.overwrite_rtif_after_execution is True

    def test_decorated_operator_template_fields(self):
        """Test that template fields are properly inherited."""

        # Create a dummy function for testing
        def dummy_callable():
            return "SELECT 1"

        operator = DuckDBDecoratedOperator(
            python_callable=dummy_callable,
            task_id="test_task",
        )

        # Check that both parent template fields are included
        assert "sql" in operator.template_fields
        assert "op_args" in operator.template_fields
        assert "op_kwargs" in operator.template_fields

    @patch("common.operators.duckdb.DuckDBHook")
    def test_decorated_operator_execute(
        self,
        mock_hook_class,
        mock_airflow_connections,
    ):
        """Test execution of decorated operator."""
        # Setup
        mock_hook = MagicMock()
        mock_result = MagicMock()
        mock_hook.sql.return_value = mock_result
        mock_hook_class.return_value = mock_hook

        def sql_callable():
            return "SELECT COUNT(*) FROM test_table"

        operator = DuckDBDecoratedOperator(
            python_callable=sql_callable,
            task_id="test_task",
        )

        # Mock the render_templates method
        operator.render_templates = MagicMock()

        # Create a mock context with task instance
        mock_ti = MagicMock()
        mock_ti.render_templates = MagicMock()
        context = {"ti": mock_ti}

        # Execute
        result = operator.execute(context)

        # Assertions
        mock_hook_class.assert_called_once()
        mock_hook.sql.assert_called_once_with("SELECT COUNT(*) FROM test_table")
        assert result == mock_result

    def test_decorated_operator_invalid_return_type(self, mock_airflow_connections):
        """Test error handling when callable returns non-string."""

        def invalid_callable():
            return 123  # Invalid return type

        operator = DuckDBDecoratedOperator(
            python_callable=invalid_callable,
            task_id="test_task",
        )

        # Mock the render_templates method
        mock_ti = MagicMock()
        mock_ti.render_templates = MagicMock()
        context = {"ti": mock_ti}

        with pytest.raises(TypeError, match="must be a non-empty string"):
            operator.execute(context)

    def test_decorated_operator_empty_string(self, mock_airflow_connections):
        """Test error handling when callable returns empty string."""

        def empty_callable():
            return ""  # Empty string

        operator = DuckDBDecoratedOperator(
            python_callable=empty_callable,
            task_id="test_task",
        )

        # Mock the render_templates method
        mock_ti = MagicMock()
        mock_ti.render_templates = MagicMock()
        context = {"ti": mock_ti}

        with pytest.raises(TypeError, match="must be a non-empty string"):
            operator.execute(context)


class TestDuckDBTaskDecorator:
    """Test the duckdb_task decorator function."""

    def test_duckdb_task_decorator(self):
        """Test that the decorator function works correctly."""

        @duckdb_task
        def test_sql_task():
            return "SELECT 1"

        # The decorator should create a DuckDBDecoratedOperator
        task_instance = test_sql_task()
        # When called, it returns an XComArg, but the operator is accessible via task
        actual_task = task_instance.operator
        assert isinstance(actual_task, DuckDBDecoratedOperator)
        assert actual_task.python_callable.__name__ == "test_sql_task"

    def test_duckdb_task_decorator_with_params(self):
        """Test decorator with custom parameters."""

        @duckdb_task(duckdb_conn_id="custom_conn", config={"memory_limit": "2GB"})
        def test_sql_task():
            return "CREATE TABLE test (id INT)"

        task_instance = test_sql_task()
        actual_task = task_instance.operator
        assert isinstance(actual_task, DuckDBDecoratedOperator)
        assert actual_task.duckdb_conn_id == "custom_conn"
        assert actual_task.config == {"memory_limit": "2GB"}

    def test_duckdb_task_in_dag_context(self, mock_airflow_connections):
        """Test using duckdb_task decorator within a DAG context."""
        with DAG(dag_id="test_dag", start_date=datetime(2023, 1, 1)) as dag:

            @duckdb_task
            def create_table():
                return """
                CREATE TABLE test_table (
                    id INTEGER,
                    name VARCHAR
                )
                """

            task = create_table()

        # In DAG context, the task is created differently
        actual_task = task.operator if hasattr(task, "operator") else task
        assert actual_task.dag == dag
        assert actual_task.task_id == "create_table"
        assert isinstance(actual_task, DuckDBDecoratedOperator)
