FROM astrocrpublic.azurecr.io/runtime:3.1-13

USER root
WORKDIR /usr/local/airflow

RUN test -f /usr/local/airflow/entrypoint-airflow-all-in-one.sh \
    && test -f /usr/local/airflow/dags/process.py \
    && test -f /usr/local/airflow/requirements.txt \
    && test -f /usr/local/airflow/packages.txt

RUN chmod +x /usr/local/airflow/entrypoint-airflow-all-in-one.sh

USER astro

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=5 \
    CMD python -c "import os, urllib.request; port = os.environ.get('AIRFLOW_API_PORT') or os.environ.get('PORT') or '8080'; urllib.request.urlopen(f'http://127.0.0.1:{port}/api/v2/version', timeout=5).read()"

CMD ["/usr/local/airflow/entrypoint-airflow-all-in-one.sh"]
