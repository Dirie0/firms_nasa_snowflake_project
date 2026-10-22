FROM apache/airflow:3.3.2
USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-snowflake \
    poetry
ENV POETRY_VIRTUALENVS_IN_PROJECT=false
ENV POETRY_VIRTUALENVS_PATH=/home/airflow/poetry-venvs
WORKDIR /opt/airflow/python_scripts
COPY --chown=airflow:root python_scripts/pyproject.toml python_scripts/poetry.lock python_scripts/README.md ./
RUN poetry install --no-root --no-interaction --no-ansi
WORKDIR /opt/airflow