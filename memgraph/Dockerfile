FROM memgraph/memgraph-platform

USER root

# Copy the local query modules
COPY query_modules/exchange.py /usr/lib/memgraph/query_modules/exchange.py
COPY query_modules/find_best_exchange.py /usr/lib/memgraph/query_modules/find_best_exchange.py
COPY query_modules/find_best_path.py /usr/lib/memgraph/query_modules/find_best_path.py

USER memgraph

# Install packages
COPY requirements.txt ./
RUN pip3 install -r requirements.txt