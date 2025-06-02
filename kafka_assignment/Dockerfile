# ./Dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential gcc wget curl librdkafka-dev && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY producer.py consumer.py faust_consumer.py location_to_borough.csv ./
COPY kafka_assignement.ipynb .

RUN pip install --no-cache-dir \
        confluent-kafka==2.3.0 \
        faust-streaming==0.11.3 \
        pandas numpy duckdb scikit-learn jupyterlab

CMD ["bash"]
