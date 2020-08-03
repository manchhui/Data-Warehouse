import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries



def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    
    # get total number of tables to be dropped
    num_tables = len(drop_table_queries)
    
    i=1
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print('Dropping (IF EXISTS) {}/{} Tables - Complete.'.format(i, num_tables))
        i+=1


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    
    # get total number of tables to be created
    num_tables = len(create_table_queries)
    
    i=1
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print('Creating {}/{} Tables - Complete.'.format(i, num_tables))
        i+=1


def main():
    """
    - Connects to the sparkifydb database on AWS Redshift Cluster.
    - Returns the connection and cursor to sparkifydb database.
    - Drops all the tables.  
    - Creates all tables needed. 
    - Finally, closes the connection. 
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print('Connection_Successful')

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()