"""
Data Loading and Insertion Script

This script provides functions to load data from staging tables into analytical tables in a database. 
The SQL queries for copying and inserting data are defined in 'sql_queries.py'. 
The script reads database connection properties from 'dwh.cfg'.

Author: SharmaRupali (https://github.com/SharmaRupali)

Please ensure that 'dwh.cfg' is properly configured before executing this script.
"""

import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data into staging tables.

    This function iterates through a list of SQL queries for copying data into staging tables and executes each query. 
    It loads data from external sources into the staging tables within the database.

    Parameters:
    cur (psycopg2.extensions.cursor): The database cursor.
    conn (psycopg2.extensions.connection): The database connection.

    Returns:
    None
    """

    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Insert data into analytical tables.

    This function iterates through a list of SQL queries for inserting data into analytical tables and executes each query. 
    It transfers data from staging tables into the analytical tables for further analysis.

    Parameters:
    cur (psycopg2.extensions.cursor): The database cursor.
    conn (psycopg2.extensions.connection): The database connection.

    Returns:
    None
    """

    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function for loading and inserting data.

    This function reads database connection properties from 'dwh.cfg', establishes a connection to the database, 
    and then calls the 'load_staging_tables' and 'insert_tables' functions to load and insert data.

    Parameters:
    None

    Returns:
    None
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