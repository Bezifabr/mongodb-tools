from itertools import islice
from pprint import pprint

from collections import Counter
from pymongo import MongoClient
from datetime import datetime

filename = "logs/" + datetime.today().strftime('%Y-%m-%d_%H%M%S') + "_result.log"


def get_database_connection(nodes, username, password, db_name, auth_source='admin', auth_mechanism='SCRAM-SHA-1'):
    client = MongoClient(
        nodes,
        username=username,
        password=password,
        authSource=auth_source,
        authMechanism=auth_mechanism
    )
    return client[db_name]


def _print_bigger_collection_name(collection_name_1, collection_name_2, collection_size_1, collection_size_2):
    with open(filename, "a") as log_file:
        if collection_size_1 > collection_size_2:
            log_file.write(collection_name_1 + " is bigger than " + collection_name_2 + "\n")
            print(collection_name_1 + " is bigger than " + collection_name_2)
        if collection_size_2 > collection_size_1:
            log_file.write(collection_name_2 + " is bigger than " + collection_name_1 + "\n")
            print(collection_name_2 + " is bigger than " + collection_name_1)


def _compare_two_collections_by_number_of_documents(collection_1, collection_2):
    collection_size_1 = collection_1.count_documents({})
    collection_size_2 = collection_2.count_documents({})
    collection_name_1: str = collection_1.name + " from " + collection_1.database.name
    collection_name_2: str = collection_2.name + " from " + collection_2.database.name
    with open(filename, "a") as log_file:
        if collection_size_2 == collection_size_1:
            log_file.write("There are " + str(collection_size_1) + " documents in both collections!\n")
            return True
        # Print number of documents
        log_file.write(collection_name_1 + " has " + str(collection_size_1) + " elements\n")
        log_file.write(collection_name_2 + " has " + str(collection_size_2) + " elements\n")
        print(collection_name_1 + " has " + str(collection_size_1) + " elements")
        print(collection_name_2 + " has " + str(collection_size_2) + " elements")
        _print_bigger_collection_name(collection_name_1, collection_name_2, collection_size_1, collection_size_2)
        return False


def _compare_two_collections_by_differing_documents(collection_1, collection_2):
    pipeline = [
        {"$group": {"_id": "$_id", "count": {"$sum": 1}}}
    ]

    A = list(collection_1.aggregate(pipeline))
    B = list(collection_2.aggregate(pipeline))

    _hashableA = list(map(lambda x: str(x["_id"]), A))
    _hashableB = list(map(lambda x: str(x["_id"]), B))

    _countedA = Counter(_hashableA)
    _countedB = Counter(_hashableB)
    counted = _countedA + _countedB

    differences: dict = {x: count for x, count in counted.items() if count < 2}

    if len(differences) == 0:
        return True
    with open(filename, "a") as log_file:
        pprint(differences, log_file)
        log_file.write("\nNumber of differences: " + str(len(differences)) + "/n")
    if len(differences) >= 10:
        pprint(list(islice(differences.items(), 10)))
        print("...")
        print("Check the log file for more details: " + str(filename))
    else:
        pprint(differences)
    print("Number of differences: " + str(len(differences)))
    return False


def run_tests(_collection_names, _db_1, _db_2):
    _general_result = True
    _error_count = 0
    _success_count = 0
    _differing_collections = []

    for collection_name_1, collection_name_2 in _collection_names:
        with open(filename, "a") as log_file:
            log_file.write('--------------------\n')
            log_file.write("Compare " + collection_name_1 + " with " + collection_name_2 + "\n")
        test1 = _compare_two_collections_by_differing_documents(_db_1[collection_name_1], _db_2[collection_name_2])
        test2 = _compare_two_collections_by_number_of_documents(_db_1[collection_name_1], _db_2[collection_name_2])

        with open(filename, "a") as log_file:
            if test1 and test2:
                log_file.write("[SUCCESS] There are no differences!\n")
                _success_count += 1
            else:
                log_file.write("[FAILED] There are some differences!\n")
                print("------------------------")
                print("[FAILED] There are some differences!")
                _error_count += 1
                _differing_collections.append((collection_name_1, collection_name_2))
                _general_result = False

    with open(filename, "a") as log_file:
        log_file.write("\n")
        log_file.write("----------------\n")

    if _general_result:
        with open(filename, "a") as log_file:
            log_file.write("[OVERALL RESULT]: SUCCESS!\n")
        with open(filename, "r") as log_file:
            print(log_file.read())
    else:
        with open(filename, "a") as log_file:
            log_file.write("[OVERALL RESULT]: FAILED!\n")
        print("[OVERALL RESULT]: FAILED!\n")

    with open(filename, "a") as log_file:
        log_file.write("Number of tests: " + str(len(_collection_names)) + "\n")
        log_file.write("Number of failed tests: " + str(_error_count) + "\n")
        log_file.write("Number of passed tests: " + str(_success_count) + "\n")
        log_file.write("-----------------------\n")
        log_file.write("Differing collections: \n")
        for col1, col2 in _differing_collections:
            log_file.write("-" + col1 + " =/= " + col2 + "\n")

    print("Number of tests: " + str(len(_collection_names)))
    print("Number of failed tests: " + str(_error_count))
    print("Number of passed tests: " + str(_success_count))
    print("-----------------------")
    print("Differing collections: ")
    for col1, col2 in _differing_collections:
        print("-" + col1 + " =/= " + col2)


#     
# USAGE EXAMPLE
# 

db_1 = get_database_connection('localhost:27017', 'user', 'password', 'database')
db_2 = get_database_connection('localhost:27018', 'user', 'password', 'database')

collection_names = [
    ['colname1', 'colname1'],
    ['colname2', 'colname2']
#   [x1, x1]
#   [x2, x2]
#   [.., ..]
#   [xn, xn]
#   etc...
]

run_tests(collection_names, db_1, db_2)
