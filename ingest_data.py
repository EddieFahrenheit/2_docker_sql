import argparse
import os
from time import time

import pandas
from sqlalchemy import create_engine


def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    # Download the CSV to a file with the name "csv_name"
    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Since our CSV is a gz file, we need to use the compression='gzip' option
    # We also need to set low_memory=False because column 3 has mixed types
    dataframe_iterator = pandas.read_csv(csv_name, iterator=True, chunksize=1000000, compression='gzip', low_memory=False)

    # Take the first chunk
    dataframe = next(dataframe_iterator)

    # Convert the date columns to datetimes
    dataframe.lpep_pickup_datetime = pandas.to_datetime(dataframe.lpep_pickup_datetime)
    dataframe.lpep_dropoff_datetime = pandas.to_datetime(dataframe.lpep_dropoff_datetime)

    # Create the table headers
    dataframe.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    # Insert the first chunk
    dataframe.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        t_start = time()

        dataframe = next(dataframe_iterator)

        dataframe.lpep_pickup_datetime = pandas.to_datetime(dataframe.lpep_pickup_datetime)
        dataframe.lpep_dropoff_datetime = pandas.to_datetime(dataframe.lpep_dropoff_datetime)

        dataframe.to_sql(name=table_name, con=engine, if_exists='append')

        t_end = time()

        print('Inserted another chunk. took %.3f seconds' % (t_end - t_start))

if (__name__ == '__main__'):

    parser = argparse.ArgumentParser(description='Ingest CSV data into Postgres')

    parser.add_argument('--user', help='user name for psotgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='table name for postgres')
    parser.add_argument('--url', help='url of the csv')

    args = parser.parse_args()

    main(args)