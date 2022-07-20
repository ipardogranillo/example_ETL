FROM apache/airflow:2.3.3
USER root

RUN pip install -r requirements.txt

# COPY /etc/ssl/certs/ca-certificates.crt usr/local/share/certs/ca-certificates.crt
# RUN chmod 644 usr/local/share/certs/ca-certificates.crt && update-ca-certificates