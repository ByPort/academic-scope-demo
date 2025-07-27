from pathlib import Path

from airflow.decorators import dag, task
from airflow.sdk import Connection

from common.operators.duckdb import duckdb_task


DUCKDB_CONFIG = {
    'memory_limit': '1GB',
    'threads': '1',
    'preserve_insertion_order': 'false',
}

DEFAULT_ARGS = {
    "retries": 1,
}


@dag(
    schedule=None,
    default_args=DEFAULT_ARGS,
    tags=["arxiv"],
    max_active_runs=1,
    max_active_tasks=1,
)
def arxiv_etl():
    @task.branch()
    def check_if_zip_exists(choices: tuple[str, str]) -> bool:
        if Path(Connection.get('raw_arxiv_zip').extra_dejson.get('path')).exists():
            return choices[0]
        return choices[1]

    @task()
    def prepare_raw_folder():
        Path(Connection.get('raw_dir').extra_dejson.get('path')).mkdir(parents=True, exist_ok=True)

    @task.bash()
    def download_zip():
        return "curl -s -L -o {{ conn.raw_arxiv_zip.extra_dejson.path }} {{ conn.http_default.host }}"

    @task()
    def skip_download_zip():
        pass

    @task.bash(trigger_rule="none_failed_min_one_success")
    def unzip():
        return "unzip -o {{ conn.raw_arxiv_zip.extra_dejson.path }} -d {{ conn.raw_dir.extra_dejson.path }}"

    @task()
    def prepare_warehouse_folder():
        Path(Connection.get('warehouse_dir').extra_dejson.get('path')).mkdir(parents=True, exist_ok=True)

    @duckdb_task(pool="warehouse_lock", config=DUCKDB_CONFIG)
    def prepare_warehouse():
        return """
        CREATE OR REPLACE TABLE publications (
            id VARCHAR,
            title VARCHAR,
            abstract VARCHAR,
            categories VARCHAR,
            update_date VARCHAR,
            submitter VARCHAR,
            PRIMARY KEY (id, update_date)
        );
        CREATE OR REPLACE TABLE authors (
            name VARCHAR
        );
        CREATE OR REPLACE TABLE publications_authors (
            publication_id VARCHAR,
            author_name VARCHAR
        );
        """

    @duckdb_task(config=DUCKDB_CONFIG)
    def extract_publications():
        return """
        COPY (
            SELECT DISTINCT
                id, title, abstract, categories, update_date, submitter
            FROM '{{ conn.raw_arxiv_json.extra_dejson.path }}'
        ) TO '{{ conn.publications_parquet.extra_dejson.path }}' (FORMAT PARQUET);
        """

    @duckdb_task(config=DUCKDB_CONFIG)
    def extract_authors():
        return """
        COPY (
            SELECT DISTINCT
                trim(array_to_string(unnest(authors_parsed), ' ')) AS name
            FROM '{{ conn.raw_arxiv_json.extra_dejson.path }}'
            WHERE authors_parsed IS NOT NULL
        ) TO '{{ conn.authors_parquet.extra_dejson.path }}' (FORMAT PARQUET);
        """

    @duckdb_task(config=DUCKDB_CONFIG)
    def extract_publications_authors():
        return """
        COPY (
            SELECT DISTINCT
                id AS publication_id,
                trim(array_to_string(unnest(authors_parsed), ' ')) AS author_name
            FROM '{{ conn.raw_arxiv_json.extra_dejson.path }}'
            WHERE authors_parsed IS NOT NULL
        ) TO '{{ conn.publications_authors_parquet.extra_dejson.path }}' (FORMAT PARQUET);
        """

    @duckdb_task(pool="warehouse_lock", config=DUCKDB_CONFIG)
    def load_publications():
        return """
        DELETE FROM publications;
        INSERT INTO publications SELECT DISTINCT * FROM '{{ conn.publications_parquet.extra_dejson.path }}';
        """

    @duckdb_task(pool="warehouse_lock", config=DUCKDB_CONFIG)
    def load_authors():
        return """
        DELETE FROM authors;
        INSERT INTO authors SELECT DISTINCT * FROM '{{ conn.authors_parquet.extra_dejson.path }}';
        """

    @duckdb_task(pool="warehouse_lock", config=DUCKDB_CONFIG)
    def load_publications_authors():
        return """
        DELETE FROM publications_authors;
        INSERT INTO publications_authors SELECT DISTINCT * FROM '{{ conn.publications_authors_parquet.extra_dejson.path }}';
        """

    check_if_zip_exists_task = check_if_zip_exists(choices=("skip_download_zip", "prepare_raw_folder"))

    prepare_raw_data = [
        check_if_zip_exists_task >> prepare_raw_folder() >> download_zip(),
        check_if_zip_exists_task >> skip_download_zip(),
    ] >> unzip()
    prepare_warehouse_ = prepare_warehouse_folder() >> prepare_warehouse()

    [prepare_raw_data, prepare_warehouse_] >> extract_publications() >> load_publications()
    [prepare_raw_data, prepare_warehouse_] >> extract_authors() >> load_authors()
    [prepare_raw_data, prepare_warehouse_] >> extract_publications_authors() >> load_publications_authors()

arxiv_etl()
