import sqlite3
import pandas as pd
import json


# Load data from the database
def load_datatable_from_db(table, columns='*', database="data/demo.db"):
    """
    Load data from the database
    :return: dataframe containing the data
    """

    if columns != '*':
        columns = ', '.join(columns)

    connection = sqlite3.connect(database)

    query = f"SELECT {columns} FROM {table}"
    table = pd.read_sql(query, connection)

    pd.read_sql(query, connection)
    connection.close()

    return table


# Load data from the terminal positions database
def load_datatable_from_terminal_db():
    """
    Load data from the database
    :return: dataframe containing the data
    """

    import pandas as pd
    import sqlite3

    # Connect to the database
    conn = sqlite3.connect('data/terminal_positions.db')

    # Query with proper table name handling
    query = 'SELECT * FROM "Query result"'  # Use double quotes or square brackets around table name
    terminals_df = pd.read_sql(query, conn)

    conn.close()
    return terminals_df


def load_query_from_db(query):
    """
    Load data from the database
    :return: dataframe containing the data
    """
    connection = sqlite3.connect("data/demo.db")

    table = pd.read_sql(query, connection)

    connection.close()

    return table


def store_dataframe_to_db(df, table):
    """
    Store the dataframe to the database
    :return: None
    """
    connection = sqlite3.connect("data/demo.db")

    df.to_sql(table, connection, if_exists='replace', index=False)

    connection.close()


def empty_database_table(table):
    """
    Empty the database table
    :return: None
    """

    connection = sqlite3.connect("data/demo.db")

    query = f"DELETE FROM {table}"

    connection.execute(query)
    # Commit the transaction
    connection.commit()

    connection.close()


def input_data_to_db(query):
    """
    Insert data to the database
    :return: None
    """

    connection = sqlite3.connect("data/demo.db")

    connection.execute(query)
    # Commit the transaction
    connection.commit()

    connection.close()

    return "Data inserted successfully"


def fill_daily_costs_table():
    """
    Daily costs can be standardized, This would mean:
    Per barge_id the daily (MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY) operating_cost are 1500
    Per barge_id the daily (MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY, SUNDAY) terminal_call_cost are 35

    :return: None
    """
    weekdays = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']
    cost_types = ['operating_cost', 'terminal_call_cost']

    connection = sqlite3.connect("data/demo.db")
    cursor = connection.cursor()
    # retrieve the barge_id from the barge table
    query = """ SELECT b.barge_id, d.barge_id FROM barges b
                LEFT JOIN daily_costs d ON b.barge_id = d.barge_id """
    cursor.execute(query)
    matches = cursor.fetchall()

    # get the unique barge_ids that are not in the daily_costs table
    unique_ids_with_none = set(id for id, match in matches if match is None)

    # insert the daily costs for the unique barge_ids
    for barge_id in unique_ids_with_none:
        for cost_type in cost_types:
            if cost_type == 'terminal_call_cost':
                cost = 35
            else:
                cost = 1500
            for weekday in weekdays:
                print(f"Inserting {cost_type} for barge_id {barge_id} on {weekday} with cost {cost}")
                query = f""" INSERT INTO daily_costs (daily_cost_type, barge_id, week_day, day_cost) 
                             VALUES ('{cost_type}', {barge_id}, '{weekday}', {cost}) """
                cursor.execute(query)
                connection.commit()

    connection.close()
    # if there are no matches, insert the daily costs
    return "successfully filled the daily_costs table"


def fill_operating_times_table():
    """

    """
    weekdays = ['MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY']
    operating_times = ['00:00:00', '23:59:59']

    connection = sqlite3.connect("data/demo.db")
    cursor = connection.cursor()

    # retrieve the barge_id from the barge table
    query = """ SELECT b.barge_id, o.barge_id FROM barges b
                LEFT JOIN operating_times o ON b.barge_id = o.barge_id """

    cursor.execute(query)
    barge_matches = cursor.fetchall()

    # get the unique barge_ids that are not in the operating_times table
    barge_unique_ids_with_none = set(id for id, match in barge_matches if match is None)

    # insert the operating times for the unique barge_ids
    for barge_id in barge_unique_ids_with_none:
        for weekday in weekdays:
            print(f"Inserting operating times for barge_id {barge_id} on {weekday} with times {operating_times}")
            query = f""" INSERT INTO operating_times (barge_id, week_day, start_time, end_time) 
                         VALUES ({barge_id}, '{weekday}', '{operating_times[0]}', '{operating_times[1]}') """
            # cursor.execute(query)
            # connection.commit()

    # retrieve the barge_id from the barge table
    query = """ SELECT o.terminal_id, t.id FROM terminals t
                LEFT JOIN operating_times o ON t.id = o.barge_id """

    cursor.execute(query)
    terminal_matches = cursor.fetchall()

    # get the unique barge_ids that are not in the operating_times table
    terminal_unique_ids_with_none = set(id for id, match in terminal_matches if match is None)

    for terminal_id in terminal_unique_ids_with_none:
        for weekday in weekdays:
            print(f"Inserting operating times for terminal_id {terminal_id} on {weekday} with times {operating_times}")
            query = f""" INSERT INTO operating_times 
                        (terminal_id, week_day, start_time, end_time, flex_start_time, flex_end_time) 
                         VALUES ({terminal_id}, '{weekday}', 
                         '{operating_times[0]}', '{operating_times[1]}', 
                         '{operating_times[0]}', '{operating_times[1]}') """
            cursor.execute(query)
            connection.commit()

    connection.close()

    return "successfully filled the operating_times table"


def vacuum_database():
    try:
        connection = sqlite3.connect("data/demo.db")
        cursor = connection.cursor()
        cursor.execute("VACUUM")
        connection.commit()
        print("Database vacuumed successfully.")
    except sqlite3.Error as e:
        print("Error vacuuming database:", e)
    finally:
        if connection:
            connection.close()


def retrieve_container_type(type):
    """
    Retrieve container type from the database

    :param type:
    :return:
    """

    # Connection to database
    connection = sqlite3.Connection("data/demo.db")
    # query to get container on isoType
    sql_query = f"SELECT * " \
                f"FROM container_types " \
                f"WHERE iso_type_code = '{type}' OR iso_type_code_1984 = '{type}' OR display_code = '{type}' "

    # retrieve_container_type
    container_type = pd.read_sql_query(sql_query, connection)
    connection.close()

    # return container_type
    return container_type


