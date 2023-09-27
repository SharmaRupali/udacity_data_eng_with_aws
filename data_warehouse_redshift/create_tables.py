"""
Database Table Management Script

This script provides functions to create and drop tables in a database based on SQL queries defined in 'sql_queries.py'. 
The script reads database connection properties from 'dwh.cfg'.

Author: SharmaRupali (https://github.com/SharmaRupali)

Please ensure that 'dwh.cfg' is properly configured before executing this script.
"""

import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables in the database.

    This function iterates through a list of SQL queries to drop tables and executes each query, 
    effectively removing the specified tables from the database.

    Parameters:
    cur (psycopg2.extensions.cursor): The database cursor.
    conn (psycopg2.extensions.connection): The database connection.

    Returns:
    None
    """
    
    for query in drop_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables in the database.

    This function iterates through a list of SQL queries to create tables and executes each query, 
    creating the specified tables in the database.

    Parameters:
    cur (psycopg2.extensions.cursor): The database cursor.
    conn (psycopg2.extensions.connection): The database connection.

    Returns:
    None
    """
    
    for query in create_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function for managing database tables.

    This function reads database connection properties from 'dwh.cfg', establishes a connection to the database, 
    and then calls the 'drop_tables' and 'create_tables' functions to manage database tables.

    Parameters:
    None

    Returns:
    None
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