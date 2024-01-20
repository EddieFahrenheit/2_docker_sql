import argparse

import pandas
from sqlalchemy import create_engine

def main(params):

    #green_taxi_trip_ingestion(params)
    taxi_zone_ingestion(params)

def green_taxi_trip_ingestion(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_name = params.url

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the whole csv, mixed types in column 3 so we need to set low_memory=False
    dataframe = pandas.read_csv(csv_name, low_memory=False)

    # Convert the first chunk's string dates types to datetimes
    dataframe.lpep_pickup_datetime = pandas.to_datetime(dataframe.lpep_pickup_datetime)
    dataframe.lpep_dropoff_datetime = pandas.to_datetime(dataframe.lpep_dropoff_datetime)

    # Write the dataframe to sql
    dataframe.to_sql(name=table_name, con=engine, if_exists='replace')

    print('Dataframe has been written to sql')

def taxi_zone_ingestion(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    csv_name = params.url

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Read the whole csv, mixed types in column 3 so we need to set low_memory=False
    dataframe = pandas.read_csv(csv_name)

    # Write the dataframe to sql
    dataframe.to_sql(name=table_name, con=engine, if_exists='replace')

    print('Dataframe has been written to sql')

if (__name__ == '__main__'):

    parser = argparse.ArgumentParser(description='Ingest CSV data into Postgres')

    parser.add_argument('--user', help='user name for psotgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')

    # Args for green taxi trip ingestion
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url of the csv')

    # Args for taxi zone ingestion
    parser.add_argument('--table_name2', help='table 2 name for postgres')
    parser.add_argument('--url2', help='url 2 of the csv')

    args = parser.parse_args()

    main(args)