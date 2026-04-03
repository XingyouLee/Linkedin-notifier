FROM astrocrpublic.azurecr.io/runtime:3.1-13-base

WORKDIR /usr/local/airflow

USER root

COPY packages.txt requirements.txt ./
RUN install-system-packages \
    && install-python-dependencies

COPY --chown=astro:0 . .

USER astro

# Install Playwright browser binaries after Python deps are available.
RUN python -m playwright install chromium

ENV AIRFLOW_SERVICE_ROLE=all-in-one \
    AIRFLOW__CORE__LOAD_EXAMPLES=False \
    PORT=8080

EXPOSE 8080

ENTRYPOINT ["bash", "scripts/start_airflow_service.sh"]
