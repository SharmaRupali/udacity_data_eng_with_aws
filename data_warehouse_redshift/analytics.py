import configparser
import psycopg2
import pandas as pd
from sql_queries import analytical_queries


def analyze(cur, conn):
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
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    analyze(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()