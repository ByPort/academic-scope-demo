from airflow.decorators import dag, task
from pathlib import Path
import os
import duckdb

RAW_ZIP = "/data/raw/arxiv.zip"
RAW_JSON = "/data/raw/arxiv-metadata-oai-snapshot.json"
RAW_DIR = "/data/raw"
WAREHOUSE_DIR = "/data/warehouse"
WAREHOUSE = f"{WAREHOUSE_DIR}/warehouse.db"

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
    @task.branch
    def check_if_zip_exists() -> bool:
        if os.path.exists(RAW_ZIP):
            return ['skip_download_zip']
        return ['download_zip']

    @task.bash
    def download_zip():
        return f'mkdir -p /data/raw && curl -L -o {RAW_ZIP} "https://www.kaggle.com/api/v1/datasets/download/Cornell-University/arxiv"'

    @task()
    def skip_download_zip():
        pass

    @task.bash(trigger_rule="none_failed_min_one_success")
    def unzip():
        return f'unzip -o {RAW_ZIP} -d {RAW_DIR}'

    @task(pool="warehouse_lock")
    def prepare_warehouse():
        Path(WAREHOUSE_DIR).mkdir(parents=True, exist_ok=True)
        with duckdb.connect(WAREHOUSE) as db:
            db.sql("""
            CREATE OR REPLACE TABLE publications (
                id VARCHAR,
                title VARCHAR,
                abstract VARCHAR,
                categories VARCHAR,
                update_date VARCHAR,
                submitter VARCHAR,
                PRIMARY KEY (id, update_date)
            )
            """)
            db.sql("""
            CREATE OR REPLACE TABLE authors (
                name VARCHAR
            )
            """)
            db.sql("""
            CREATE OR REPLACE TABLE publications_authors (
                publication_id VARCHAR,
                author_name VARCHAR
            )
            """)

    @task()
    def extract_publications():
        # shutil.rmtree(f"{RAW_DIR}/publications", ignore_errors=True)
        # Path(f"{RAW_DIR}/publications").mkdir(parents=True, exist_ok=True)

        # for i, df in enumerate(pd.read_json(RAW_JSON, lines=True, chunksize=10000)):
        #     df['id'] = df['id'].astype(str)
        #     df = df[['id', 'title', 'abstract', 'categories', 'update_date', 'submitter']]
        #     df.to_parquet(f"{RAW_DIR}/publications/{i}.parquet")

        with duckdb.connect(config=DUCKDB_CONFIG) as db:
            db.sql(f"""
            COPY (
                SELECT DISTINCT
                    id, title, abstract, categories, update_date, submitter
                FROM '{RAW_JSON}'
            ) TO '{RAW_DIR}/publications.parquet' (FORMAT PARQUET);
            """)

    @task()
    def extract_authors():
        # shutil.rmtree(f"{RAW_DIR}/authors", ignore_errors=True)
        # Path(f"{RAW_DIR}/authors").mkdir(parents=True, exist_ok=True)

        # for i, df in enumerate(pd.read_json(RAW_JSON, lines=True, chunksize=10000)):
        #     df['id'] = df['id'].astype(str)
        #     df = (
        #         df['authors_parsed']
        #         .dropna()
        #         .explode()
        #         .apply(lambda author: ' '.join(author).strip())
        #         .drop_duplicates()
        #         .to_frame(name='name')
        #     )
        #     df.to_parquet(f"{RAW_DIR}/authors/{i}.parquet")

        with duckdb.connect(config=DUCKDB_CONFIG) as db:
            db.sql(f"""
            COPY (
                SELECT DISTINCT
                    trim(array_to_string(unnest(authors_parsed), ' ')) AS name
                FROM '{RAW_JSON}'
                WHERE authors_parsed IS NOT NULL
            ) TO '{RAW_DIR}/authors.parquet' (FORMAT PARQUET);
            """)

    @task()
    def extract_publications_authors():
        # shutil.rmtree(f"{RAW_DIR}/publications_authors", ignore_errors=True)
        # Path(f"{RAW_DIR}/publications_authors").mkdir(parents=True, exist_ok=True)

        # for i, df in enumerate(pd.read_json(RAW_JSON, lines=True, chunksize=10000)):
        #     df['id'] = df['id'].astype(str)
        #     df = (
        #         df[['id', 'authors_parsed']]
        #         .dropna(subset=['authors_parsed'])
        #         .explode('authors_parsed')
        #         .assign(author_name=lambda x: x['authors_parsed'].apply(lambda author: ' '.join(author).strip()))
        #         [['id', 'author_name']]
        #         .rename(columns={'id': 'publication_id'})
        #     )
        #     df.to_parquet(f"{RAW_DIR}/publications_authors/{i}.parquet")

        with duckdb.connect(config=DUCKDB_CONFIG) as db:
            db.sql(f"""
            COPY (
                SELECT DISTINCT
                    id AS publication_id,
                    trim(array_to_string(unnest(authors_parsed), ' ')) AS author_name
                FROM '{RAW_JSON}'
                WHERE authors_parsed IS NOT NULL
            ) TO '{RAW_DIR}/publications_authors.parquet' (FORMAT PARQUET);
            """)

    @task(pool="warehouse_lock")
    def load_publications():
        with duckdb.connect(WAREHOUSE, config=DUCKDB_CONFIG) as db:
            db.sql("DELETE FROM publications")
            db.sql(f"INSERT INTO publications SELECT DISTINCT * FROM '{RAW_DIR}/publications.parquet'")

    @task(pool="warehouse_lock")
    def load_authors():
        with duckdb.connect(WAREHOUSE, config=DUCKDB_CONFIG) as db:
            db.sql("DELETE FROM authors")
            db.sql(f"INSERT INTO authors SELECT DISTINCT * FROM '{RAW_DIR}/authors.parquet'")

    @task(pool="warehouse_lock")
    def load_publications_authors():
        with duckdb.connect(WAREHOUSE, config=DUCKDB_CONFIG) as db:
            db.sql("DELETE FROM publications_authors")
            db.sql(f"INSERT INTO publications_authors SELECT DISTINCT * FROM '{RAW_DIR}/publications_authors.parquet'")

    extract_publications_task = extract_publications()
    extract_authors_task = extract_authors()
    extract_publications_authors_task = extract_publications_authors()

    data_processing = [
        extract_publications_task,
        extract_authors_task,
        extract_publications_authors_task,
    ]

    prepare_warehouse() >> data_processing
    check_if_zip_exists() >> [download_zip(), skip_download_zip()] >> unzip() >> data_processing

    extract_publications_task >> load_publications()
    extract_authors_task >> load_authors()
    extract_publications_authors_task >> load_publications_authors()

arxiv_etl()
