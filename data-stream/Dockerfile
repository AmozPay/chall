FROM python:3.8

# Install CMake for gqlalchemy
RUN apt-get update && \
  apt-get --yes install cmake && \
  rm -rf /var/lib/apt/lists/*

# Install packages
COPY requirements.txt ./
RUN pip3 install -r requirements.txt

COPY prices_producer.py /app/prices_producer.py
COPY data_ingest_mockup.py /app/data_ingest_mockup.py
COPY setup.py /app/setup.py
COPY prices-data /app/prices-data

WORKDIR /app