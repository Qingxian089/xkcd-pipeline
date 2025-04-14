FROM apache/airflow:2.10.5
USER root

# 安装系统依赖（psycopg2需要）
RUN apt-get update && \
    apt-get install -y gcc python3-dev && \
    rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt