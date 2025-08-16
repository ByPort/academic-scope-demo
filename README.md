# Academic Scope Demo

A complete ETL pipeline demonstration using Apache Airflow to process academic publication data from the arXiv metadata dataset. This project showcases modern data engineering practices including workflow orchestration, efficient data processing with DuckDB, and containerized development environments.

**What it does:** Downloads, transforms, and loads academic publication data into a data warehouse, extracting publications, authors, and their relationships for analysis.

## 🚀 Quick Start

### Prerequisites
- Docker and Docker Compose
- Python 3.12+ (for development only)

### Setup & Run
1. **Clone and navigate to the project**
    ```bash
    git clone https://github.com/ByPort/academic-scope-demo
    cd academic-scope-demo
    ```

2. **Set up `.env` file**
    ```bash
    cp .env.example .env
    ```

3. **Configure data directory (if not in Dev Container)**
    ```bash
    echo "HOST_DATA_DIR=$HOME/academic-scope-data" >> .env
    ```

4. **Optional: Enable monitoring and alerting**
    ```bash
    echo "AIRFLOW_ENABLE_METRICS=True" >> .env
    echo "COMPOSE_PROFILES=monitoring" >> .env
    cp config/alertmanager/config.yaml.example config/alertmanager/config.yaml
    ```
    Configure webhooks for Discord and/or Slack in `config/alertmanager/config.yaml`

5. **Start Airflow**
    ```bash
    docker compose up -d --no-recreate
    ```
    
6. **Set up pools and connections**
    ```bash
    cat config/airflow/pools.json | docker compose run --rm -T airflow-cli pools import /dev/stdin

    # /dev/stdin doesn't work for connections for some reason
    cat config/airflow/connections.json | docker compose run --rm -T airflow-cli bash -c 'cat > /tmp/connections.json && airflow connections import /tmp/connections.json'
    ```

7. **Start the pipeline**
    ```bash
    ./airflow.sh dags trigger arxiv_etl
    ```

8. **Monitor progress**
   - **Airflow UI**: `http://localhost:8080` (credentials: `airflow`/`airflow`)
   - **Grafana Dashboard**: `http://localhost:3000` (default credentials: `admin`/`admin`)
   - **Prometheus Metrics**: `http://localhost:9090`
   - Watch the `arxiv_etl` DAG execution in the Graph or Grid view

## 💻 Working with the Pipeline

### Airflow Management
Run Airflow commands using the helper script:

```bash
# Access Airflow CLI
./airflow.sh

# Common commands
./airflow.sh dags list
./airflow.sh tasks list arxiv_etl
./airflow.sh dags trigger arxiv_etl
```

### Development Setup
Install development packages:
```bash
pip install -r requirements-dev.txt
```

Run tests:
```bash
    PYTHONPATH=$PYTHONPATH:$(pwd)/airflow/dags pytest
```

### Data Exploration

Once the pipeline completes, explore the processed data:

**Using Python:**

```python
import duckdb

# Connect to the warehouse
conn = duckdb.connect('data/warehouse/warehouse.db')

# Explore the data
conn.sql("SELECT COUNT(*) FROM publications").show()
conn.sql("SELECT COUNT(*) FROM authors").show()
conn.sql("SELECT COUNT(*) FROM publications_authors").show()

# Find most prolific authors
conn.sql("""
    SELECT author_name, COUNT(DISTINCT publication_id) AS pub_count
    FROM publications_authors
    GROUP BY author_name
    ORDER BY pub_count DESC
    LIMIT 10
""").show()
```

**Using DuckDB CLI:**

```bash
duckdb data/warehouse/warehouse.db
```

```sql
SELECT author_name, COUNT(DISTINCT publication_id) AS pub_count
FROM publications_authors
GROUP BY author_name
ORDER BY pub_count DESC
LIMIT 10;
```

## 📁 Project Structure

```
academic-scope-demo/
├── .devcontainer/               # Dev container configuration
├── .github/workflows/ci.yml     # CI/CD pipeline configuration
├── airflow/
│   ├── config/airflow.cfg       # Airflow configuration
│   ├── dags/
│   │   ├── arxiv_etl.py         # Main ETL DAG
│   │   └── common/              # Shared components (hooks, operators)
│   ├── logs/                    # Airflow task logs
│   └── plugins/                 # Custom Airflow plugins
├── config/                      # Monitoring and alerting configuration
│   ├── airflow/                 # Airflow connections and pools
│   ├── alertmanager/            # Alert routing configuration
│   ├── grafana/                 # Dashboards and datasource config
│   ├── prometheus/              # Metrics collection and alerting rules
│   └── statsd/                  # StatsD exporter mappings
├── data/
│   ├── raw/                     # Raw data files
│   └── warehouse/               # DuckDB warehouse
├── tests/                       # Test suite (pytest)
├── .dockerignore                # Docker build ignore patterns
├── .env.example                 # Environment variables template
├── .gitignore                   # Git ignore patterns
├── airflow.Dockerfile           # Airflow service Docker image
├── airflow.sh                   # Airflow CLI helper script
├── docker-compose.airflow.yml   # Complete Airflow stack
├── pyproject.toml               # Python tools configurations
├── requirements.txt             # Python dependencies
├── requirements-dev.txt         # Python dev dependencies
└── README.md                    # This file
```

## 🛠️ Technical Details

### Pipeline Architecture
The ETL pipeline (`arxiv_etl` DAG) orchestrates 10 tasks:

1. **check_if_zip_exists**: Verifies if the dataset is already downloaded
2. **prepare_raw_folder**: Creates `data/raw` folder
3. **download_zip**: Downloads the arXiv metadata dataset from Kaggle
4. **unzip**: Extracts the JSON file from the downloaded archive
5. **prepare_warehouse_folder**: Creates `data/warehouse` folder
6. **prepare_warehouse**: Initializes the DuckDB database schema
7. **extract_publications**: Processes publication metadata into Parquet format
8. **extract_authors**: Extracts unique author names into Parquet format
9. **extract_publications_authors**: Creates publication-author relationship mappings
10. **load_publications**: Imports publications data into the warehouse
11. **load_authors**: Imports authors data into the warehouse
12. **load_publications_authors**: Imports relationship mappings into the warehouse

### Technologies Used

- **Apache Airflow 3.0.3**: Workflow orchestration platform
- **DuckDB**: Fast analytical database for data processing
- **Python 3.12**: Primary programming language
- **Docker & Dev Containers**: Containerization for consistent development environment
- **Parquet**: Columnar storage format for intermediate data
- **Prometheus**: Metrics collection and alerting rules engine
- **Grafana**: Data visualization and dashboards
- **Alertmanager**: Alert routing and notification management
- **StatsD Exporter**: Airflow metrics collection
- **Node Exporter**: System metrics collection

### Data Processing

The pipeline processes the arXiv metadata which includes:
- **Publications**: Research paper metadata (title, abstract, categories, etc.)
- **Authors**: Unique list of all authors
- **Relationships**: Many-to-many relationships between publications and authors

### Performance Considerations

- **Memory Requirements**: At least 8 GB RAM dedicated to Docker for Airflow stack and data processing
- **DuckDB Configuration**: Configured with 1 GB memory limit for efficient processing
- **Data Format**: Uses Parquet format for optimized storage and processing
- **Concurrency**: Implements resource pools for warehouse operations (DuckDB supports only one writer at a time)

## 🎓 Learning Objectives

This project demonstrates:
- Building ETL pipelines with Apache Airflow
- Working with real-world datasets
- Data processing with modern analytical databases
- Containerized development workflows
- Metrics collection and visualization with Prometheus and Grafana
- Discord and Slack ready alerting with Alertmanager

## 📝 License

This project is for demonstration purposes and is not affiliated with arXiv or Apache Airflow.

## 🚧 Future Improvements

- [ ] Add data quality checks and validation
- [x] Add automated testing for DAG tasks
- [ ] Showcase incremental data loading
- [ ] Include more advanced data transformations
- [ ] Implement log aggregation with ELK stack
