import requests
import json
import streamlit as st

basic_url = st.secrets["BOS_URL"]
authentication = (st.secrets["BOS_AUTH"][0], st.secrets["BOS_AUTH"][1])

def cof_push_voyages(payload):
    # Test the endpoint with a manufactured dataset. See if that will work

    post_voyages_url = basic_url + 'v1/voyages'
    headers = {'Content-Type': 'application/json'}
    post_voyages_req = requests.post(post_voyages_url, auth=authentication, data=payload, headers=headers)

    return post_voyages_req.text

def push_cargo(payload):
    # Test the endpoint with a manufactured dataset. See if that will work

    post_orders_url = basic_url + 'api/danser/orders'
    headers = {'Content-Type': 'application/json'}

    post_orders_req = requests.post(post_orders_url, auth=authentication, data=payload, headers=headers)

    return print(post_orders_req.text)

def push_voyages(payload):
    # Test the endpoint with a manufactured dataset. See if that will work

    post_voyages_url = basic_url + 'api/danser/voyages'
    headers = {'Content-Type': 'application/json'}
    post_voyages_req = requests.post(post_voyages_url, auth=authentication, data=payload, headers=headers)

    return post_voyages_req.text


def delete_all_containers():
    """
    Delete all containers in the database

    :return:
    """

    page = 0
    page_substance = True

    while page_substance:
        page += 1

        # Getting 10 containers at a time
        get_containers_url = f'{basic_url}v1/cargos'
        headers = {'Content-Type': 'application/json'}
        get_containers_req = requests.get(get_containers_url, auth=authentication, headers=headers)
        get_containers_json = get_containers_req.json()

        # check if the length of the content is 0
        if len(get_containers_json['content']) == 0:
            page_substance = False
            print(f'Page {page} has been processed')
            break

        # Retrieve the id of the containers
        id_list = [record['id'] for record in get_containers_json['content']]
        print(f'The total containers to delete: {record["totalElements"]}')

        for id in id_list:
            delete_containers_url = f'{basic_url}v1/cargos/{id}'
            headers = {'Content-Type': 'application/json'}
            delete_containers_req = requests.delete(delete_containers_url, auth=authentication, headers=headers)


def get_container_pages():
    # Getting 10 containers at a time
    get_containers_url = f'{basic_url}v1/cargos'
    headers = {'Content-Type': 'application/json'}
    get_containers_req = requests.get(get_containers_url, auth=authentication, headers=headers)
    get_containers_json = get_containers_req.json()

    return get_containers_json['totalPages']

def get_number_of_voyages():
    # Getting 10 containers at a time
    get_voyages_url = f'{basic_url}v1/voyages'
    headers = {'Content-Type': 'application/json'}
    get_voyages_req = requests.get(get_voyages_url, auth=authentication, headers=headers)
    get_voyages_json = get_voyages_req.json()

    return get_voyages_json['totalElements']

def get_number_of_calls():
    # Getting 10 containers at a time
    get_calls_url = f'{basic_url}v1/calls'
    headers = {'Content-Type': 'application/json'}
    get_calls_req = requests.get(get_calls_url, auth=authentication, headers=headers)
    get_calls_json = get_calls_req.json()

    return get_calls_json['totalResults']

def delete_a_page_of_containers():
    get_containers_url = f'{basic_url}v1/cargos'
    headers = {'Content-Type': 'application/json'}
    get_containers_req = requests.get(get_containers_url, auth=authentication, headers=headers)
    get_containers_json = get_containers_req.json()

    # check if the length of the content is 0
    if len(get_containers_json['content']) == 0:
        page_substance = False
        print(f'Page {page} has been processed')
        return None

    # Retrieve the id of the containers
    id_list = [record['id'] for record in get_containers_json['content']]
    for id in id_list:
        delete_containers_url = f'{basic_url}v1/cargos/{id}'
        headers = {'Content-Type': 'application/json'}
        delete_containers_req = requests.delete(delete_containers_url, auth=authentication, headers=headers)

    return get_containers_json['totalElements'] - len(id_list)


def delete_all_calls():
    """
    Delete all calls in the database

    :return:
    """

    page = 0
    page_substance = True

    while page_substance:
        page += 1
        get_calls_url = f'{basic_url}v1/calls'
        headers = {'Content-Type': 'application/json'}
        get_calls_req = requests.get(get_calls_url, auth=authentication, headers=headers)
        get_calls_json = get_calls_req.json()
        print(get_calls_json)
        id_list = [record['id'] for record in get_calls_json['results']]

        for id in id_list:
            print(id)
            delete_calls_url = f'{basic_url}v1/calls/{id}'
            headers = {'Content-Type': 'application/json'}
            delete_calls_req = requests.delete(delete_calls_url, auth=authentication, headers=headers)
            print(delete_calls_req.text)

        if len (id_list) == 0:
            page_substance = False
            print(f'Page {page} has been processed')


def delete_all_voyages():
    """
    Delete all containers in the database

    :return:
    """

    page = 0
    page_substance = True

    while page_substance:
        page += 1
        get_voyages_url = f'{basic_url}v1/voyages'
        headers = {'Content-Type': 'application/json'}
        get_voyages_req = requests.get(get_voyages_url, auth=authentication, headers=headers)
        get_voyages_json = get_voyages_req.json()
        print(get_voyages_json)
        id_list = [record['id'] for record in get_voyages_json['content']]

        for id in id_list:
            print(id)
            delete_voyages_url = f'{basic_url}v1/voyages/{id}'
            headers = {'Content-Type': 'application/json'}
            delete_voyages_req = requests.delete(delete_voyages_url, auth=authentication, headers=headers)
            print(delete_voyages_req.text)

        if len (id_list) == 0:
            page_substance = False
            print(f'Page {page} has been processed')


def get_ship_positions(fleet_id):
    """

    :param fleet_id:
    :return:
    """

    get_fleet_position_url = f'{basic_url}v1/positions/{str(fleet_id)}'
    get_fleet_position_req = requests.get(get_fleet_position_url, auth=authentication)
    get_fleet_position_json = get_fleet_position_req.json()

    return get_fleet_position_json