FROM python:3.9.1

RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_local_csv.py ingest_local_csv.py

ENTRYPOINT ["python", "ingest_local_csv.py"]