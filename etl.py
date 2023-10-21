import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """Iterate over list of load staging tables queries.
    Load the data from the S3 bucket to the staging tables in Redshift
    
    Args:
        cur: cursor connection object to database used to execute commands.
        conn: psycopg2 object that handles The connection to the database
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Iterates execution  over the list of insert queries.
    load the data from staging tables to final analytics tables on Redshift.
   
    Args:
        cur: cursor connection object to database used to execute commands.
        conn: psycopg2 object that handles The connection to the database
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
     main method that reads the db credentials from the config file, 
     establish DWH connection to database, 
     call load and insert functions that loads the s3 files into staging tables and final tables from staging tables, 
     finally, close the db connection.
     """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()