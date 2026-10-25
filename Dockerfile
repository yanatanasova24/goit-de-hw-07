FROM apache/airflow:2.10.5

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-mysql==5.7.2 \
    mysql-connector-python
