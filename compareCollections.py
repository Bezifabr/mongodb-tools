from pymongo import MongoClient


def get_collection_from_database(nodes, username, password, db_name, collection_name, auth_source='admin',
                                 auth_mechanism='SCRAM-SHA-1'):
    client = MongoClient(
        nodes,
        username=username,
        password=password,
        authSource=auth_source,
        authMechanism=auth_mechanism
    )
    return client[db_name][collection_name]


def _get_unique_documents_from_collection(base_collection_cursor, comparison_collection, field_name):
    return [doc for doc in base_collection_cursor if
            not (bool(comparison_collection.find_one({field_name: doc.get(field_name)})))]


def _print_list_of_differing_documents(collection_name, differing_documents):
    if differing_documents:
        print("List of differing documents from " + collection_name)
        for doc in differing_documents:
            print(doc)


def _print_number_of_differing_documents(collection_name_1, collection_name_2, difference):
    print(collection_name_1 + " has " + str(difference) + " documents that do not exist in " + collection_name_2)


def _print_number_of_documents(collection_name, number_of_documents):
    print(collection_name + " has " + str(number_of_documents) + " elements")


def _print_one_is_bigger_than_other(name_1, name_2):
    print(name_1 + " is bigger than " + name_2)


def _print_bigger_collection(collection_name_1, collection_name_2, collection_size_1, collection_size_2):
    if collection_size_1 > collection_size_2:
        _print_one_is_bigger_than_other(collection_name_1, collection_name_2)
    if collection_size_2 > collection_size_1:
        _print_one_is_bigger_than_other(collection_name_2, collection_name_1)


def compare_two_collections_by_field_sorted_by_date(collection_1, collection_2, unique_field_name, date_field):
    all_documents_cursor_1 = collection_1.find({}).sort(date_field)
    all_documents_cursor_2 = collection_2.find({}).sort(date_field)
    differing_documents_1 = _get_unique_documents_from_collection(all_documents_cursor_1, collection_2,
                                                                  unique_field_name)
    differing_documents_2 = _get_unique_documents_from_collection(all_documents_cursor_2, collection_1,
                                                                  unique_field_name)

    collection_size_1 = collection_1.count_documents({})
    collection_size_2 = collection_2.count_documents({})
    collection_name_1: str = collection_1.name + " from " + collection_1.database.name
    collection_name_2: str = collection_2.name + " from " + collection_2.database.name

    # Print differing documents
    _print_list_of_differing_documents(collection_name_1, differing_documents_1)
    _print_list_of_differing_documents(collection_name_2, differing_documents_2)
    print('--------------')
    # Print number of documents
    _print_number_of_documents(collection_name_1, collection_size_1)
    _print_number_of_documents(collection_name_2, collection_size_2)
    _print_bigger_collection(collection_name_1, collection_name_2, collection_size_1, collection_size_2)
    # Print number of differences
    _print_number_of_differing_documents(collection_name_1, collection_name_2, len(differing_documents_1))
    _print_number_of_differing_documents(collection_name_2, collection_name_1, len(differing_documents_2))

    print("ONLY IF THE COMPARED COLLECTIONS HAVE DOCUMENTS WITH DATE FIELDS")
    print("The oldest differing document from the " + collection_name_1 + ": " + str(differing_documents_1[0]))
    print("The newest differing document from the " + collection_name_1 + ": " + str(differing_documents_1[-1]))
    print("The oldest differing document from the " + collection_name_2 + ": " + str(differing_documents_2[0]))
    print("The newest differing document from the " + collection_name_2 + ": " + str(differing_documents_2[-1]))

#     
# USAGE EXAMPLE
# 

collection_1 = get_collection_from_database(
    'localhost:27017', 'user', 'password', 'database', 'collection')
collection_2 = get_collection_from_database(
    'localhost:27018', 'user', 'password', 'database', 'collection')

compare_two_collections_by_field_sorted_by_date(collection_1, collection_2, 'id_field', 'date_field')
