FROM apache/airflow:2.9.3-python3.10

USER root
# Install system packages required for spatial and netcdf libraries
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential \
       libpq-dev \
       libhdf5-dev \
       libnetcdf-dev \
       gdal-bin \
       libgdal-dev \
       curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
# Copy requirements and install python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r /requirements.txt
