import json
import random
import pandas as pd
from datetime import datetime, timedelta
import sqlite3

timestamp = datetime.now()
next_midnight = (timestamp + timedelta(days=2)).replace(hour=0, minute=0, second=0, microsecond=0)
previous_midnight = timestamp.replace(hour=0, minute=0, second=0, microsecond=0)


def generate_container_data(proportions, total_teu, sea_terminals, inland_terminals):
    # Step 2: Generate the container information
    # Number of containers
    num_containers = {
        1: int(total_teu * proportions[0] / 1),
        2: int(total_teu * proportions[1] / 2),
        2.25: int(total_teu * proportions[2] / 2.25)
    }

    # Adjust for any rounding issues to ensure total TEU is 5000
    while sum([k * v for k, v in num_containers.items()]) != total_teu:
        num_containers[1] += 1

    containers = []
    for size, count in num_containers.items():
        containers.extend([size] * count)
    random.shuffle(containers)

    # Step 3: Determine Pickup and Delivery Locations
    container_data = []

    # Helper function to generate grouped container attributes
    def generate_container_group(container_group_sizes):
        if random.random() < 0.8:  # 80% for sea terminals
            if random.random() < 0.5:
                pickup = random.choice(sea_terminals)
                delivery = random.choice(inland_terminals)
            else:
                pickup = random.choice(inland_terminals)
                delivery = random.choice(sea_terminals)
        else:  # 20% for inland terminals
            pickup = random.choice(inland_terminals)
            delivery = random.choice([t for t in inland_terminals if t != pickup])

        earliest_pickup = random.randint(0, 6)  # Earliest pickup time within the week
        latest_delivery = random.randint(earliest_pickup + 3,
                                         earliest_pickup + 7)  # Latest delivery time within the week

        no_of_containers = random.choice(container_group_sizes)

        booking_number = random.randint(120000, 130000)

        return pickup, delivery, earliest_pickup, latest_delivery, no_of_containers, booking_number

    container_group_sizes = [1, 2, 4, 5, 7, 8, 10, 15, 18, 30, 35, 40, 42, 50, 65, 73, 88, 91, 95, 100, 111, 133, 142,
                             150]
    diff = len(containers) - sum(container_group_sizes)
    extra_size1 = int(0.175 * diff)
    extra_size2 = int(0.33 * diff)
    extra_size3 = diff - extra_size1 - extra_size2
    container_group_sizes.append(extra_size1)
    container_group_sizes.append(extra_size2)
    container_group_sizes.append(extra_size3)
    # Generate grouped containers
    all_locations = sea_terminals + inland_terminals
    externalIds = {}
    k = 0
    for location in all_locations:
        k = k + 1
        externalIds[location] = k
    while len(containers) > 0:
        pickup, delivery, earliest_pickup, latest_delivery, no_of_containers, booking_number = generate_container_group(
            container_group_sizes)
        container_group_sizes.remove(no_of_containers)
        no_of_containers = min(no_of_containers, len(containers))
        l = 1
        for container in range(no_of_containers):
            size = random.choice(containers)
            containers.remove(size)
            if size == 2:
                container_type = '40DV'
                weight = random.choice([3870, random.randint(5000, 7500)])
            elif size == 2.25:
                container_type = '45HC'
                weight = 5050
            else:
                container_type = '20TK'
                weight = random.choice([2200, random.randint(3000, 4400)])

            latest_delivery = max(earliest_pickup + 1, latest_delivery)
            earliest = (next_midnight + timedelta(days=earliest_pickup)).strftime('%Y-%m-%dT%H:%M:%SZ')
            latest = (next_midnight + timedelta(days=latest_delivery)).strftime('%Y-%m-%dT%H:%M:%SZ')
            container_data.append({
                'bookingReference': str(booking_number) + '-' + str(l),
                'bookingDateCreated': timestamp.strftime('%d %m %Y'),
                'containerNumber': f'C{len(container_data) + 1:05}',
                'containerType': container_type,
                'teu': size,
                'weight': weight,
                'reefer': False,
                'dangerousGoods': False,
                'loadTerminal': pickup,
                'loadExternalId': externalIds[pickup],
                'loadTimeWindowStart': earliest,
                'loadTimeWindowEnd': latest,
                'dischargeTerminal': delivery,
                'dischargeExternalId': externalIds[delivery],
                'dischargeTimeWindowStart': earliest,
                'dischargeTimeWindowEnd': latest
            })
            l = l + 1

    container_df = pd.DataFrame(container_data)
    print(container_df)
    # Connect to the database (or create it if it doesn't exist)
    conn = sqlite3.connect(r"data\demo.db")

    # Save the DataFrame to the 'users' table in the SQLite3 database
    container_df.to_sql('container_orders', conn, if_exists='replace', index=False)

    # Close the connection
    conn.close()

# Step 5: Define the Barge Fleet
# barges = []
# capacities = [112, 198, 200, 150, 280]
# b = 101
# for i in range(5):
#     capacity = random.choice(capacities)
#     capacities.remove(capacity)
#     barges.append({
#         'id': f'B{i+1}',
#         'externalId': b,
#         'capacityTEU': capacity,
#         'capacityWeight': 1000000,
#         'capacityReefer': capacity,
#         'capacityDangerGoods': capacity,
#         'kilometerCost': int(capacity/20),
#         'speed': 12.5,
#         'dayCost': {
#             'MONDAY': 0,
#             'TUESDAY': 0,
#             'WEDNESDAY': 0,
#             'THURSDAY': 0,
#             'FRIDAY': 0,
#             'SATURDAY': 0,
#             'SUNDAY': 500
#         },
#         "terminalCallCost": {
#             "MONDAY": 50,
#             "TUESDAY": 50,
#             "WEDNESDAY": 50,
#             "THURSDAY": 50,
#             "FRIDAY": 50,
#             "SATURDAY": 50,
#             "SUNDAY": 500
#         },
#         'stops': [
#             {
#                 'terminalId': random.choice(sea_terminals),
#                 'lineStopId': random.randint(185000000, 190000000),
#                 'loadOrders': [],
#                 'dischargeOrders': [],
#                 'timeWindow': {
#                     'startDateTime': previous_midnight.strftime('%Y-%m-%dT%H:%M:%SZ'),
#                     'endDateTime': previous_midnight.strftime('%Y-%m-%dT%H:%M:%SZ')
#                 },
#                 'fixedStop': True
#             }
#         ],
#         "activeTimes": [
#             {
#                 "weekDay": "MONDAY",
#                 "startTime": "00:00:00",
#                 "endTime": "23:59:59"
#             },
#             {
#                 "weekDay": "TUESDAY",
#                 "startTime": "00:00:00",
#                 "endTime": "23:59:59"
#             },
#             {
#                 "weekDay": "WEDNESDAY",
#                 "startTime": "00:00:00",
#                 "endTime": "23:59:59"
#             },
#             {
#                 "weekDay": "THURSDAY",
#                 "startTime": "00:00:00",
#                 "endTime": "23:59:59"
#             },
#             {
#                 "weekDay": "FRIDAY",
#                 "startTime": "00:00:00",
#                 "endTime": "23:59:59"
#             },
#             {
#                 "weekDay": "SATURDAY",
#                 "startTime": "00:00:00",
#                 "endTime": "23:59:59"
#             },
#             {
#                 "weekDay": "SUNDAY",
#                 "startTime": "00:00:00",
#                 "endTime": "23:59:59"
#             }
#         ],
#         "forbiddenTerminals": []
#     })
#     b = b + 1
#
# barges_df = pd.DataFrame(barges)
#
# # Step 6: Format the Dataset
# # Save the dataframes to a json file
# data = {
#     'webhook': {
#         'url': '',
#         'token': ''
#     },
#     'mailhook': {
#         'emailAddress': '',
#         'token': ''
#     },
#     'timestamp': timestamp.strftime('%Y-%m-%dT%H:%M:%SZ'),
#     'appointments': [],
#     'orders': container_data,
#     'terminals': location_data,
#     'vessels': barges
# }
#
# # Specify the file name
# t = timestamp.strftime('%d%m%y_%H%M')
# filename = 'dummy_data_PMA' + t + '.json'
#
# # Write the dictionary to a JSON file
# with open(filename, 'w') as file:
#     json.dump(data, file, indent=4)  # `indent=4` for pretty printing
