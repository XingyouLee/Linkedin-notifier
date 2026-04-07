FROM astrocrpublic.azurecr.io/runtime:3.1-13

USER root
WORKDIR /usr/local/airflow

RUN chmod +x \
    /usr/local/airflow/entrypoint-airflow-init.sh \
    /usr/local/airflow/entrypoint-airflow-webserver.sh \
    /usr/local/airflow/entrypoint-airflow-scheduler.sh \
    /usr/local/airflow/entrypoint-airflow-dag-processor.sh \
    /usr/local/airflow/entrypoint-airflow-triggerer.sh \
    /usr/local/airflow/entrypoint-airflow-all-in-one.sh

USER astro

CMD ["/usr/local/airflow/entrypoint-airflow-all-in-one.sh"]
