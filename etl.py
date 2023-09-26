# importing required packages
import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# function that loads data into each staging table
def load_staging_tables(cur, conn):
    """
    Load data into staging tables using a list of SQL queries.
    This function executes a series of SQL queries provided in 
    `copy_table_queries` list to load data into staging tables.

    Parameters:
        cur (cursor): The database cursor.
        conn (connection): The database connection.

    Example:
        load_staging_tables(cursor, connection)
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
        print(copy_table_queries.index(query))

# function that inserts the data from the staging tables into final tables
def insert_tables(cur, conn):
    """
    Insert data into tables using a list of SQL queries.
    This function executes a series of SQL queries from the
    'insert_table_queries' list, which is defined in the 'sql_queries' module.
    
    Parameters:
        cur (cursor): The database cursor.
        conn (connection): The database connection.

    Example:
        insert_tables(cursor, connection)
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

# main function for our ETL purposes
def main():
    """
    Executes the ETL process by connecting to a Redshift cluster,
    loading data into staging tables, and then inserting the data into
    final tables based on configurations located in 'dwh.cfg'.

    This function performs the following steps:
        -- Reads the configuration file 'dwh.cfg' using ConfigParser.
        -- Makes a connection to the Redshift cluster.
        -- Creates a cursor for executing SQL queries on the database.
        -- Loads data into the staging tables using 'load_staging_tables' function.
        -- Inserts data into the final tables using the 'insert_tables' function.
        -- Closes the database connection.

    Example:
        main()
    """
    # reads the file 'dwh.cfg'
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    # connects to the redshift cluster using the values in the configuration
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    # loads the data into the staging tables
    load_staging_tables(cur, conn)
    # inserts the data into the final tables
    insert_tables(cur, conn)
    # closes the connection 
    conn.close()


if __name__ == "__main__":
    main()