import duckdb
from airflow.hooks.base import BaseHook


class DuckDBHook(BaseHook):
    """
    Hook to interact with DuckDB databases.

    This hook provides methods to execute SQL commands against a DuckDB database.
    It can be used for data manipulation, querying, and other database operations.

    :param duckdb_conn_id: The connection ID for the DuckDB database.
    :param config: Optional configuration for the DuckDB connection.
    """

    conn_name_attr = "duckdb_conn_id"
    default_conn_name = "duckdb_default"
    conn_type = "fs"
    hook_name = "DuckDBHook"

    def __init__(
        self,
        duckdb_conn_id=default_conn_name,
        config: dict | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.log.info(
            "Initializing DuckDBHook with connection ID %s and config %s",
            duckdb_conn_id,
            config,
        )
        self.duckdb_conn_id = duckdb_conn_id
        self.config = config
        self.database = self.get_conn().extra_dejson.get("path")
        self.log.info("DuckDBHook database path: %s", self.database)

    def get_conn(self):
        return self.get_connection(self.duckdb_conn_id)

    def sql(self, sql: str):
        with duckdb.connect(database=self.database, config=self.config) as conn:
            return conn.sql(sql)
