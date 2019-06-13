'''******************************************************************************
Importing libraries
******************************************************************************'''

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

'''******************************************************************************
Functions
******************************************************************************'''

def load_staging_tables(cur, conn):
    
    '''
    Function that create temporary tables, that are used to load tables of interest
    
    Input:
    cur -->   A SQL cursor
    conn --> Connection to the database
    '''
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    
    '''
    Function to load data to tables of interests, using previously created staging tables
    
    Input:
    cur -->   A SQL cursor
    conn --> Connection to the database
    '''    
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

'''******************************************************************************
Main
******************************************************************************'''

def main():
    
    #Creating and configuring configparser
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    #Creating a connection to the db
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #Calling functions create staging tables and to load data to tables of interest
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    #Closing connection
    conn.close()


if __name__ == "__main__":
    main()