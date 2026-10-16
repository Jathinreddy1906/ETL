FROM apache/airflow:2.10.5
USER airflow
RUN pip install --no-cache-dir requests==2.32.3 psycopg2-binary==2.9.10
