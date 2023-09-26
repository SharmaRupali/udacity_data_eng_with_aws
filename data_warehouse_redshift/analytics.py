import configparser
import psycopg2
from sql_queries import analytical_queries


def analyze(cur, conn):
    for query in analytical_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    analyze(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()