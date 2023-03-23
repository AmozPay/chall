FROM python:3.10

WORKDIR /app

COPY requirements.txt .
RUN python3 -m pip install -r ./requirements.txt

COPY kafka_consumer.py /app/kafka_consumer.py
COPY setup.py /app/setup.py

CMD python3 kafka_consumer.py