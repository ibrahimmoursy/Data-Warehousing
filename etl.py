import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copy S3 data into staging tables on the Redshift cluster.

    Iterates over a list of SQL COPY commands stored in `copy_table_queries`,
    executing each one to load data from S3 into staging tables in Redshift.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert values into the Star Schema.

    Iterates over a list of SQL COPY commands stored in `insert_table_queries`,
    executing each one to insert values from staging tables into the star schema.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Makes the connection to the Database redshift using the redshift credentials in `dwh.cfg` file.

    It also uses the `load_staging_tables` function to copy data into staging tables
    and uses `insert_tables` function to insert the values into the star Schema in the Redshift Cluster.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print('Connecting to Redshift...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Loading Staging Tables...')
    load_staging_tables(cur, conn)
    
    print('Inserting Tables in Redshift...')
    insert_tables(cur, conn)
    
    print('Done')
    conn.close()


if __name__ == "__main__":
    main()