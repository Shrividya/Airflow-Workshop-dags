FROM apache/airflow:3.2.2

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    poppler-utils \
    tesseract-ocr \
    tesseract-ocr-eng \
    libmagic1 \
    libgl1 \
    libglib2.0-0 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt
COPY data /data
RUN pip install --no-cache-dir -r /requirements.txt
RUN pip install --no-cache-dir apache-airflow-providers-common-ai
RUN pip install "chromadb>=0.5.0"
