from pprint import pprint

from collections import Counter
from pymongo import MongoClient


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
    if collection_size_1 > collection_size_2:
        pprint(collection_name_1 + " is bigger than " + collection_name_2)
    if collection_size_2 > collection_size_1:
        pprint(collection_name_2 + " is bigger than " + collection_name_1)


def _compare_two_collections_by_number_of_documents(collection_1, collection_2):
    collection_size_1 = collection_1.count_documents({})
    collection_size_2 = collection_2.count_documents({})
    collection_name_1: str = collection_1.name + " from " + collection_1.database.name
    collection_name_2: str = collection_2.name + " from " + collection_2.database.name

    if collection_size_2 == collection_size_1:
        pprint("There are " + str(collection_size_1) + " documents in both collections!")
        return True
    # Print number of documents
    pprint(collection_name_1 + " has " + str(collection_size_1) + " elements")
    pprint(collection_name_2 + " has " + str(collection_size_2) + " elements")
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
    pprint(differences)
    print("Number of differences: " + str(len(differences)))
    return False


def run_tests(_collection_names, _db_1, _db_2):
    _general_result = True
    for collection_name_1, collection_name_2 in _collection_names:
        print()
        print("--------------------")
        pprint("Compare " + collection_name_1 + " with " + collection_name_2)
        test1 = _compare_two_collections_by_differing_documents(_db_1[collection_name_1], _db_2[collection_name_2])
        test2 = _compare_two_collections_by_number_of_documents(_db_1[collection_name_1], _db_2[collection_name_2])
        if test1 and test2:
            print("[SUCCESS] There are no differences!")
        else:
            print("[FAILED] There are some differences!")
            _general_result = False

    print()
    print()
    print("________________")
    if _general_result:
        pprint("[OVERALL RESULT]: SUCCESS!")
    else:
        pprint("[OVERALL RESULT]: FAILED!")
    print("Number of tests: " + str(len(_collection_names)))
    print("----------------")
    print()
    print()

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
