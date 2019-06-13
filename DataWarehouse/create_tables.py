'''******************************************************************************
Importing libraries
******************************************************************************'''

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

'''******************************************************************************
Functions
******************************************************************************'''

def drop_tables(cur, conn):
    
    '''
    Function to drop in case of exist the tables to create
    
    Input:
    cur -->   A SQL cursor
    conn --> Connection to the database
    '''
    
    #Dropping tables
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    
    '''
    Function that create tables of interest
    
    Input:
    cur -->   A SQL cursor
    conn --> Connection to the database
    '''
    
    #Creating tables
    for query in create_table_queries:
        #print(query, '\n')
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

    #Dropping and creating tables of interest
    drop_tables(cur, conn)
    create_tables(cur, conn)

    #Closing connection
    conn.close()


if __name__ == "__main__":
    main()