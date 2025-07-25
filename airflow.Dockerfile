ARG AIRFLOW_IMAGE_NAME

FROM $AIRFLOW_IMAGE_NAME

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends unzip \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
USER airflow

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
