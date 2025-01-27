import pandas as pd
import time
import argparse
from sqlalchemy import create_engine
import os 
import wget
import gzip
import shutil
from time import time

def get_connection(user,password,database,port,host):
    try:
        return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{database}")
    except Exception as e:
        print(f"Exception occured in Database connection creation:- {str(e)}")


# def unzip_gz(csv_name):
#     file_name = csv_name.split(".")
#     file_name = file_name[0] + ".csv"
#     with gzip.open(csv_name, 'rb') as f_in, open(file_name, 'wb') as f_out:
#         shutil.copyfileobj(f_in,f_out)
#     print(f"Unzipped file {file_name} to {os.getcwd()}")


def ingest_data(url, *params):
    # table_name = "_".join([val for val in url.split("/")[-1].split("_") if not val.isdigit()])
    conn = get_connection(*params)
    if ".gz" in url:
        table_name = "green_trip"
        # csv_name = f"green_trip.csv.gz"
    else:
        table_name = "taxi_zone_lookup"
        # csv_name = f"taxi_zone_lookup.csv"
    # wget.download(url,csv_name)
    if ".gz" in url:
        # unzip_gz(csv_name)
        df_iter = pd.read_csv(url,iterator=True,compression='gzip', chunksize=100000)
    else:
        df_iter = pd.read_csv(url,iterator=True, chunksize=100000)
    create_table_flag = False
    count = 0
    for df in df_iter:
        try:
            if not create_table_flag:
                df.head(n=0).to_sql(name=table_name, con=conn, index=False, if_exists='replace')
                create_table_flag = True
            if "tpep_pickup_datetime" in df.columns: df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            if "tpep_dropoff_datetime" in df.columns: df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            df.to_sql(name=table_name, con=conn, index=False, if_exists='append')
            count += len(df)
        except StopIteration as e:
            print(f"Stopiteration error has occured")
        except Exception as e:
            print(f"Error:- {str(e)}")
    print(f"Total records loaded in table = {count}")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data to postgres")
    parser.add_argument("--user", help="user name for postgres")
    parser.add_argument("--password", help="password for postgres")
    parser.add_argument("--database", help="database for postgres")
    parser.add_argument("--table_name", help="table  name in postgres")
    parser.add_argument("--port", help="port for postgres")
    parser.add_argument("--host", help="host for postgres")
    parser.add_argument("--url", help="url for postgres")

    args = parser.parse_args()
    url_list = args.url
    if len(url_list) == 0:
        print(f"url list can not be empty!!")
        exit(1)
    conn_params = [args.user, args.password, args.database, args.port, args.host]
    start_time = time()
    for url in url_list.split(","):
        ingest_data(url, *conn_params)
    end_time = time()
    print(f"Overall process took {end_time - start_time} seconds..")