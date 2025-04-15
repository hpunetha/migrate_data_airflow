FROM apache/airflow:2.5.1

COPY requirements.txt .
COPY .env .
COPY config.json .

RUN pip install --no-cache-dir -r requirements.txt

COPY dags/ /opt/airflow/dags/
COPY entrypoint.sh /entrypoint.sh

USER root
RUN chmod +x /entrypoint.sh
USER airflow

ENTRYPOINT ["/entrypoint.sh"]
