import configparser
import psycopg2
from sql_queries import copy_table_queries, drop_table_queries, create_table_queries, insert_table_queries, drop_staging_table_queries

def drop_tables(cur, conn, drop_table_queries):
    """
    This function will be responsible for deleting pre-existing tables to ensure that our database does not throw any error if we try creating a table that already exists.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
def create_tables(cur, conn):
    """
    This function will be responsible for creating tables to ensure that our database does not throw any error if we try inserting data into tables.
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        

def load_tables(cur, conn):
    """
    This function will be responsible for loading data from S3 into staging table
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        
def change_timestamp(cur, conn):
    """
    This function will be responsible for changing the date related column.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    drop_tables(cur, conn, drop_table_queries)
    print('All table dropped!')
    create_tables(cur, conn)
    print('All table created!')
    load_tables(cur, conn)
    change_timestamp(cur, conn)
    drop_tables(cur, conn, drop_staging_table_queries)
    print('All table loaded!')

    conn.close()


if __name__ == "__main__":
    main()
