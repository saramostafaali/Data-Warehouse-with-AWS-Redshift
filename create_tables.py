import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """execute the drop tables queries.
     iterates over all the drop table queries and executes them.
    
     Args:
         cur: cursor connection object to database used to execute commands.
         conn: psycopg2 object that handles The connection to the database.
     """ 
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
     
    #for query in drop_table_queries:
        #cur.execute(query)
       # conn.commit()


def create_tables(cur, conn):
    """execute the create tables queries.
     iterates over all the create table queries and executes them.
    
     Args:
         cur: cursor connection object to database used to execute commands.
         conn: psycopg2 object that handles The connection to the database.
     """ 
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()



def main():
    """"
    reads the db credentials from the config file, 
    establish DWH connection to database, 
    drops and creates the all tables, 
    finally, close the db connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()