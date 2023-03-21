FROM memgraph/memgraph-platform

USER root

# Copy the local query modules
COPY query_modules/exchange.py /usr/lib/memgraph/query_modules/exchange.py

USER memgraph

# Install packages
COPY requirements.txt ./
RUN pip3 install -r requirements.txt