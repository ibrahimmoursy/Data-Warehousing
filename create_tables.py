import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    
    """
    Drops all table using the queries in `drop_table_queries` list.

    It is used, so that runing multiple times makes each time as first time creating tables.
    """
   
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
  
    """
    Creates tables using the queries in `create_table_queries` list 

    Iterates over a list of SQL Create TABLE commands stored in `create_table_queries`,
    executing each one to create tables for the staging area and the Redshift cluster.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    Makes the connection to the Database redshift using the redshift credentials in `dwh.cfg` file 

    It also uses the `drop_table` function to drop tables and uses `create_table` function to create the tables needed for the project
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')


    print('Connecting to redshift ...')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    print('Droping Excisting Tables ...')
    drop_tables(cur, conn)
    
    print('Creating Tables ...')
    create_tables(cur, conn)
    print('Done')
    conn.close()


if __name__ == "__main__":
    main()