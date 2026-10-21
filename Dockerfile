FROM apache/airflow:2.9.3-python3.10@sha256:1f395499be0fbc834e0a8a634754715a3938b08ca161b11cf9c837e4372aa9d2 AS runtime

# The pinned base already provides curl for the webserver healthcheck.
# Native Python dependencies use wheels; no extra compiler/GDAL CLI is needed.
USER airflow
# Copy requirements and install python packages
COPY requirements.txt /requirements.txt
# requirements.txt also pins apache-airflow to the base image version.
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r /requirements.txt \
    && pip check

# Test tools and synthetic fixtures never enter the runtime target.
FROM runtime AS test
COPY requirements-dev.txt /requirements-dev.txt
RUN pip install --no-cache-dir --only-binary=:all: --require-hashes -r /requirements-dev.txt \
    && pip check
WORKDIR /workspace
COPY scripts/ ./scripts/
COPY dags/ ./dags/
COPY sql/init_dwh_schema.sql ./sql/init_dwh_schema.sql
COPY sql/02_olap_views.sql ./sql/02_olap_views.sql
COPY tests/ ./tests/
COPY pyproject.toml ./pyproject.toml
ENV WEATHER_TESTING=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    AIRFLOW__CORE__LOAD_EXAMPLES=false \
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/weather-test-airflow.db
ENTRYPOINT ["python", "-m", "pytest"]
CMD ["-m", "not integration"]
