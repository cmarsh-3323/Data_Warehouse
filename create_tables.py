# importing the required packages
import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# function that drops all existing tables
def drop_tables(cur, conn):
    """
    This function takes a database cursor and connection object,
    and iterates through a list of SQL queries for dropping tables,
    which are defined in the 'sql_queries.py' module.

    Parameters:
        cur (cursor): The database cursor.
        conn (connection): The database connection.

    Example:
        drop_tables(cursor, connection)
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        print(drop_table_queries.index(query))

# function that creates new tables
def create_tables(cur, conn):
    """
    This function takes a database cursor and connection object,
    and iterates through a list of SQL queries for creating the tables
    which are defined in the 'sql_queries.py' module.
    
    Parameters:
        cur (cursor): The database cursor.
        conn (connection): The database connection.

    Example:
        create_tables(cursor, connection)
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        print(create_table_queries.index(query))

# main function to run the ETL process
def main():
    """
    This main function executes the ETL process, which involves the following steps:
        - Extracting database credentials from the 'dwh.cfg' configuration file.
        - Establishing a connection to the Redshift cluster using the extracted credentials.
        - Dropping pre-existing tables using the 'drop_tables' function.
        - Creating new tables using the 'create_tables' function.
        - Closing the connection with the Redshift cluster.

    Example:
        main()
    """
    # reads file 'dwh.cfg'
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # connects to the redshift cluster using the values within the configuration
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # drops the tables
    drop_tables(cur, conn)
    # creates new tables
    create_tables(cur, conn)
    # closes the connection 
    conn.close()


if __name__ == "__main__":
    main()