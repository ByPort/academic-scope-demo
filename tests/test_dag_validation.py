import pytest


class TestDagValidation:
    """Test basic DAG validation across all DAGs."""

    def test_no_import_errors(self, dagbag):
        """Test that all DAG files can be imported without errors."""
        assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"

    def test_dag_bag_size(self, dagbag):
        """Test that we have at least one DAG."""
        assert len(dagbag.dag_ids) >= 1, f"Expected at least 1 DAG, found {len(dagbag.dag_ids)}"

    def test_dags_loaded(self, dagbag):
        """Test that all DAGs can be loaded without errors."""
        for dag_id in dagbag.dag_ids:
            dag = dagbag.get_dag(dag_id)
            assert dag is not None, f"DAG {dag_id} could not be loaded"


class TestArxivETLDag:
    """Test class for arxiv_etl DAG validation."""

    @pytest.fixture(scope="class")
    def dag(self, dagbag):
        """Get the arxiv_etl DAG."""
        return dagbag.get_dag(dag_id="arxiv_etl")

    def test_dag_loaded(self, dag):
        """Test that the DAG is loaded without errors."""
        assert dag is not None
        assert dag.dag_id == "arxiv_etl"

    def test_dag_structure(self, dag):
        """Test DAG structure and task count."""
        assert dag is not None
        assert len(dag.tasks) > 0
        assert len(dag.tasks) >= 10  # Expected minimum task count

    @pytest.mark.parametrize(
        "expected_task_id",
        [
            "prepare_raw_folder",
            "check_if_zip_exists", 
            "download_zip",
            "unzip",
            "extract_publications",
            "extract_authors",
            "extract_publications_authors",
            "prepare_warehouse_folder",
            "prepare_warehouse",
            "load_publications",
            "load_authors",
            "load_publications_authors",
        ],
    )
    def test_task_exists(self, dag, expected_task_id):
        """Test that expected tasks exist in the DAG."""
        assert dag is not None
        task_ids = [task.task_id for task in dag.tasks]
        assert expected_task_id in task_ids

    def test_dag_schedule(self, dag):
        """Test DAG schedule configuration."""
        assert dag is not None
        # This DAG is set to manual triggering (schedule=None)
        assert dag.schedule is None

    def test_dag_max_active_runs(self, dag):
        """Test DAG max_active_runs configuration."""
        assert dag is not None
        assert isinstance(dag.max_active_runs, int)
        assert dag.max_active_runs >= 1

    def test_dag_max_active_tasks(self, dag):
        """Test DAG max_active_tasks configuration."""
        assert dag is not None
        assert isinstance(dag.max_active_tasks, int)
        assert dag.max_active_tasks >= 1

    def test_task_dependencies(self, dag):
        """Test task dependencies."""
        assert dag is not None

        # Get tasks by ID
        tasks = {task.task_id: task for task in dag.tasks}
        
        # Check that key tasks exist
        assert "check_if_zip_exists" in tasks
        assert "prepare_warehouse_folder" in tasks
        assert "prepare_warehouse" in tasks
        assert "extract_publications" in tasks
        assert "load_publications" in tasks
        
        # Verify basic structure - all tasks should be connected to the DAG
        assert all(task.dag == dag for task in dag.tasks)
