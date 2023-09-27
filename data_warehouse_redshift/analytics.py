"""
Database Analysis Script

This script provides functions to execute analytical queries on analytical tables in a database. 
The SQL queries for analyzing the data are defined in 'sql_queries.py'. 
The script reads database connection properties from 'dwh.cfg'.

Author: SharmaRupali (https://github.com/SharmaRupali)

Please ensure that 'dwh.cfg' is properly configured before executing this script.
"""

import configparser
import psycopg2
import pandas as pd
from sql_queries import analytical_queries


def analyze(cur, conn):
    """
    Execute a series of analytical SQL queries.

    This function takes a cursor (cur) and a database connection (conn) as input.
    It iterates through a list of analytical SQL queries executes each query,
    and displays the result as a DataFrame on the console.

    Parameters:
    cur (psycopg2.extensions.cursor): The database cursor.
    conn (psycopg2.extensions.connection): The database connection.

    Returns:
    None
    """
    
    for query in analytical_queries:
        print(query)
        cur.execute(query)
        result = cur.fetchall()

        if result:
            # If the query returns results, create a DataFrame and display it
            df = pd.DataFrame(result, columns=[desc[0] for desc in cur.description])
            print("\nResult DataFrame:")
            print(df)
        else:
            print("No results returned.\n")


def main():
    """
    Main function to connect to the database and execute analytical queries.

    This function reads database connection parameters from a configuration file,
    establishes a connection to the database, and calls the 'analyze' function
    to execute analytical queries. After processing, it closes the database connection.

    Parameters:
    None

    Returns:
    None
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    analyze(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()