FROM mcr.microsoft.com/devcontainers/python:1-3.12-bullseye

COPY requirements.txt /
COPY requirements-dev.txt /
RUN pip install --no-cache-dir -r /requirements-dev.txt
