FROM python:3.12.8
RUN pip install pandas sqlalchemy wget psycopg2
WORKDIR /app
COPY data_ingest.py data_ingest.py
CMD ["python","data_ingest.py","--user","root","--password","root","--host","pgdatabase","--port","5432","--database","ny_taxi", \
 "--table_name","yellow_taxi_trips","--url","https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"]
