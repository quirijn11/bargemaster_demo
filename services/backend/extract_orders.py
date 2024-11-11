import pandas
import pandas as pd
import openpyxl
import json

def extract_orders(orders_input, file_type=None):
    """
    Check the file type and handle accordingly.

    :param orders_input:
    :return:
    """

    if file_type == "xlsx":
        orders = pd.read_excel(orders_input)
        return orders
    elif file_type == "csv":
        # Check wich separator is used
        orders = pd.read_csv(orders_input)
        return orders
    else:
        return None

def extract_orders_xlsx(orders_input):
    """
    If end users uploads an exce file, should transform to a dataframe
    :param orders_input:
    :return:
    """

    # Check if the file is an excel file
    if orders_input.endswith(".xlsx"):
        orders = pd.read_excel(orders_input)
        return orders
    else:
        return None


def extract_orders_json(json_input):
    """
    If end users uploads an exce file, should transform to a dataframe
    :param orders_input:
    :return:
    """

    appointments = pd.read_json(json_input["appointments"])
    orders = pd.read_json(json_input["orders"])
    terminals = pd.read_json(json_input["terminals"])
    vessels = pd.read_json(json_input["vessels"])

    return appointments, orders, terminals, vessels









