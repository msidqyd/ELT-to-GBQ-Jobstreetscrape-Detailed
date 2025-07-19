# Use official Airflow image with Python 3.11
FROM apache/airflow:2.8.1-python3.11

USER root

# Install required packages including Chromium & dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        chromium \
        chromium-driver \
        fonts-liberation \
        libnss3 \
        libxss1 \
        libappindicator1 \
        libatk-bridge2.0-0 \
        libgtk-3-0 \
        libasound2 \
        libx11-xcb1 \
        wget \
        unzip \
        curl && \
    apt-get autoremove -y && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Link Chromium binary to expected location
RUN ln -sf /usr/bin/chromium /usr/bin/google-chrome

USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    selenium \
    google-cloud-bigquery \
    pandas \
    pyarrow
