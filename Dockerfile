FROM python:3.12.8
RUN pip install pandas sqlalchemy wget psycopg2
WORKDIR /app
COPY data_ingest.py data_ingest.py
CMD ["python","data_ingest.py","--user","root","--password","root","--host","pgdatabase","--port","5432","--database","ny_taxi", \
 "--url","https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz,https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv"]
