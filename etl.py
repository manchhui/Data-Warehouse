import configparser
import psycopg2
import pandas as pd
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """
    Load data from S3 into Staging Tables.
    """
    
    # get total number of tables to be loaded
    num_tables = len(copy_table_queries)

    i=1
    for query in copy_table_queries:
        print('Extracting Data & Loading {}/{} Staging Tables - In Progress'.format(i, num_tables))
        cur.execute(query)
        conn.commit()
        i+=1
        
    # data cleanup
    print('Clean Loaded Data - In Progress')
    cur.execute("""DELETE FROM staging_logs WHERE userId IS NULL;""")
    cur.execute("""DELETE FROM staging_logs WHERE sessionId IS NULL;""")
    cur.execute("""DELETE FROM staging_logs WHERE ts IS NULL;""")
    cur.execute("""DELETE FROM staging_songs WHERE song_id IS NULL;""")
    cur.execute("""DELETE FROM staging_songs WHERE artist_id IS NULL;""")
    conn.commit()

def insert_tables(cur, conn):
    """
    Insert Data From Staging Tables into Star Schema Tables.
    """
    
    # get total number of tables to be loaded
    num_tables = len(insert_table_queries)
    
    i=1
    for query in insert_table_queries:
        print('Transforming Data & Loading {}/{} Star Scheme Tables - In Progress'.format(i, num_tables))
        cur.execute(query)
        conn.commit()
        i+=1

def main():
    """
    - Connects to the sparkifydb database on AWS Redshift Cluster.
    - Returns the connection and cursor to sparkifydb database.
    - Load data from S3 into staging tables.  
    - Insert data from staging tables into sparkifydb database. 
    - Finally, closes the connection. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connection Successful')
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print('ETL Process Complete')
    conn.close()

if __name__ == "__main__":
    main()