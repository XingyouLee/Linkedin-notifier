FROM astrocrpublic.azurecr.io/runtime:3.1-13

USER root
WORKDIR /usr/local/airflow

COPY packages.txt /tmp/packages.txt
RUN apt-get update \
    && xargs -a /tmp/packages.txt apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY . /usr/local/airflow

RUN chmod +x \
    /usr/local/airflow/entrypoint-airflow-init.sh \
    /usr/local/airflow/entrypoint-airflow-webserver.sh \
    /usr/local/airflow/entrypoint-airflow-scheduler.sh \
    /usr/local/airflow/entrypoint-airflow-dag-processor.sh \
    /usr/local/airflow/entrypoint-airflow-triggerer.sh \
    /usr/local/airflow/entrypoint-airflow-all-in-one.sh

USER astro

CMD ["/usr/local/airflow/entrypoint-airflow-all-in-one.sh"]
