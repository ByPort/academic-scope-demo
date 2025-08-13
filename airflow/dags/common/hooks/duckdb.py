from typing import Any

import duckdb
from airflow.exceptions import AirflowNotFoundException
from airflow.hooks.base import BaseHook
from airflow.sdk import Connection


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
        duckdb_conn_id: str = default_conn_name,
        config: dict | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.log.info(
            "Initializing DuckDBHook with connection ID %s and config %s",
            duckdb_conn_id,
            config,
        )
        self.duckdb_conn_id = duckdb_conn_id
        self.config = config
        path = self.get_conn().extra_dejson.get("path")
        if path is None or not isinstance(path, str):
            msg = (
                f"The connection '{self.duckdb_conn_id}' does not have "
                "a valid 'path' in its extra field."
            )
            raise AirflowNotFoundException(msg)
        self.database = path
        self.log.info("DuckDBHook database path: %s", self.database)

    def get_conn(self) -> Connection:
        return self.get_connection(self.duckdb_conn_id)

    def sql(self, sql: str) -> duckdb.DuckDBPyRelation:
        config = self.config or {}
        with duckdb.connect(database=self.database, config=config) as conn:
            return conn.sql(sql)
