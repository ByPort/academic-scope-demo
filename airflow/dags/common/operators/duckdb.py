from collections.abc import Callable, Collection, Mapping
from typing import Any, ClassVar

from airflow.sdk import BaseOperator
from airflow.sdk.bases.decorator import (
    DecoratedOperator,
    TaskDecorator,
    task_decorator_factory,
)
from airflow.sdk.definitions._internal.types import (
    SET_DURING_EXECUTION,  # noqa: PLC2701
)
from airflow.utils.context import context_merge
from airflow.utils.operator_helpers import determine_kwargs

from common.hooks.duckdb import DuckDBHook


class DuckDBOperator(BaseOperator):
    """
    Operator to execute DuckDB SQL queries.

    This operator allows you to run SQL commands against a DuckDB database.
    It can be used for data manipulation, querying, and other database operations.

    :param sql: The SQL query to execute.
    :param duckdb_conn_id: The connection ID for the DuckDB database.
    """

    template_fields = ("sql",)
    template_fields_renderers: ClassVar[dict] = {"sql": "sql"}

    xcom_push = None

    def __init__(
        self,
        sql,
        duckdb_conn_id=DuckDBHook.default_conn_name,
        config: dict | None = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.duckdb_conn_id = duckdb_conn_id
        self.config = config

    def execute(self, context):
        duckdb_hook = DuckDBHook(duckdb_conn_id=self.duckdb_conn_id, config=self.config)

        self.log.info("Executing SQL: %s", self.sql)
        result = duckdb_hook.sql(self.sql)

        if result:
            self.log.info("Query executed successfully, result: %s", result)

        return result


class DuckDBDecoratedOperator(DecoratedOperator, DuckDBOperator):
    template_fields = (
        *DecoratedOperator.template_fields,
        *DuckDBOperator.template_fields,
    )
    template_fields_renderers: ClassVar[dict] = {
        **DecoratedOperator.template_fields_renderers,
        **DuckDBOperator.template_fields_renderers,
    }

    custom_operator_name = "@duckdb_task"
    overwrite_rtif_after_execution = True

    def __init__(
        self,
        *,
        python_callable: Callable,
        multiple_outputs: bool | None = None,
        op_args: Collection[Any] | None = None,
        op_kwargs: Mapping[str, Any] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            python_callable=python_callable,
            multiple_outputs=multiple_outputs,
            op_args=op_args,
            op_kwargs=op_kwargs,
            sql=SET_DURING_EXECUTION,
            **kwargs,
        )

    def execute(self, context) -> Any:
        context_merge(context, self.op_kwargs)
        kwargs = determine_kwargs(self.python_callable, self.op_args, context)

        self.sql = self.python_callable(*self.op_args, **kwargs)

        if not isinstance(self.sql, str) or not self.sql.strip():
            msg = (
                "The returned value from the TaskFlow callable "
                "must be a non-empty string."
            )
            raise TypeError(msg)

        context["ti"].render_templates()

        return super().execute(context)


def duckdb_task(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    **kwargs,
) -> TaskDecorator:
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=DuckDBDecoratedOperator,
        **kwargs,
    )
