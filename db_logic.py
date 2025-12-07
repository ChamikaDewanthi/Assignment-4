import pymongo
import psycopg
import shlex
import pandas as pd
from collections import OrderedDict

# postgres DB Connection
postgres_db = psycopg.connect(host="localhost", port="5432", dbname="Aurora", user="postgres", password="admin")
cursor = postgres_db.cursor()
postgres_tables = cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ").fetchall()
postgres_table_count = len(postgres_tables)

# mongo DB Connection
mongo_connection = pymongo.MongoClient("mongodb://localhost:27017")
mongo_db = mongo_connection["Aurora"] 
mongo_collections = mongo_db.list_collection_names()

# Function to send list of tables
def get_tables():
    table_list = []
    # Append tables from mongo db to list
    for table in mongo_collections:
        table_list.append(table)
    # Append tables from postgres db to list
    for table in postgres_tables:
        if not(table[0] in table_list):
            table_list.append(table[0])
    return table_list

# Function to perform operation
def perform_operation(table, operation, query):
    results = None
    if operation == 'print':
        results = print_tables(table)
    elif operation == 'query':
        results = query_handler(query)
    elif operation == 'insert':
        results = insert_handler(query)
    elif operation == 'update':
        results = update_handler(query)
    elif operation == 'delete':
        results = delete_handler(query)
    return results

# Function to return a consolidated data list from both databases
def print_tables(table):
    print_list = []
    mongo_results = get_mongo_table(table)
    postgres_results = get_postgres_table(table)
    if mongo_results != -1 and postgres_results != -1:
        print_list = concetenate_data(mongo_results, postgres_results)
        return print_list
    elif mongo_results != -1:
        return mongo_results
    elif postgres_results != -1:
        return postgres_results
    else:
        return

# Function to return results for a query from both databases
def query_handler(query):
    # Validate for a select query
    if shlex.split(query)[0].lower() != "select":
        return "This operation can be used only for select queries"
    # Error handling
    mongo_results = mongo_query_handler(query)
    postgres_results = postgres_query_handler(query)
    if mongo_results != -1 and postgres_results != -1:
        results = concetenate_data(mongo_results, postgres_results)
        return results
    elif mongo_results != -1:
        return mongo_results
    elif postgres_results != -1:
        return postgres_results
    else:
        return "Invalid query"

# Function to insert data to one of two databases
def insert_handler(query):
    # Validate for an insert query
    split_query = shlex.split(query)
    if split_query[0].lower() != "insert" or split_query[1].lower() != "into":
        return "This operation can be used only for insert queries"
    # Get number of records available for the given table in both databases
    table = split_query[2]
    if table in mongo_collections:
        mongo_count = mongo_db[table].count_documents({})
    else:
        mongo_count = -1
    postgres_count = None
    for table_name in postgres_tables:
        if table_name[0] == table:
            with postgres_db.cursor() as cursor:
                cursor.execute(f"SELECT COUNT(*) FROM {table};")
                postgres_count = cursor.fetchone()[0]
                cursor.close()
    if not postgres_count:
        postgres_count = -1
    # Call insert function on the table with less records by comparing both databases
    if (mongo_count > postgres_count or mongo_count == -1) and postgres_count != -1:
        results = postgres_insert_handler(query)
        if results == 0:
            results = "Insert is unseccessful"
    elif (postgres_count >= mongo_count  or postgres_count == -1) and mongo_count != -1:
        results = mongo_insert_handler(query)
    else:
        results = -1
    # Error handling
    if (results == -1):
        return "Invalid query"
    else:
        return results

# Function to update data
def update_handler(query):
    # Validate for an update query
    if shlex.split(query)[0].lower() != "update":
        return "This operation can be used only for update queries"
    mongo_results = mongo_update_handler(query)
    postgres_results = postgres_update_handler(query)
    if mongo_results != -1 and postgres_results != -1 and postgres_results != 0:
        results = concetenate_data(mongo_results, postgres_results)
    elif mongo_results != -1:
        results = mongo_results
    elif postgres_results == 0:
        results = "No data found"
    elif postgres_results != -1:
        results = postgres_results
    else:
        results = "Invalid query"
    return results

# Function to delete data
def delete_handler(query):
    # Validate for a delete query
    split_query = shlex.split(query)
    if split_query[0].lower() != "delete" or split_query[1].lower() != "from":
        return "This operation can be used only for delete queries"
    results_mongo = mongo_delete_handler(query)
    results_postgres = postgres_delete_handler(query)
    # Set deleted records count
    if results_mongo >=0 and results_postgres >= 0:
        deleted_count = results_mongo + results_postgres
        return str(deleted_count) + " records deleted"
    elif results_postgres > 0:
        return str(results_postgres) + " records deleted"
    elif results_mongo > 0:
        return str(results_mongo) + " records deleted"
    # Error handling
    elif results_mongo == -1 and results_postgres == -1:
        return "Invalid query"

# Function to get list of data from mongodb table
def get_mongo_table(table):
    collection = mongo_db[table]
    data = collection.find({}, {"_id":0})
    results = list(data)
    return results

# Function to get list of data from postgres table
def get_postgres_table(table):
    try:
        # Build query to fetch all data
        for item in postgres_tables:
            if table == item[0]:
                table_name = table
        if table_name == None:
            return -1
        SQL_query = "SELECT * FROM " + table_name + ";"
        # Fetch data
        cursor = postgres_db.cursor()
        cursor.execute(SQL_query)
        data = cursor.fetchall()
        # Format results to return with column headings
        column_names = [desc[0] for desc in cursor.description]
        data_frame = pd.DataFrame(data, columns=column_names)
        results = data_frame.to_dict(orient="records")
        cursor.close()
    except:
        results = -1
    return results

# Function to get results for a query on mongodb table
def mongo_query_handler(query):
    try:
        # Split query to seperate output attributes, collection and where conditions
        split_query = shlex.split(query)
        index_from = 0
        index_where = 0
        i = 0
        for word in split_query:
            if word.lower() == "from":
                index_from = i
            if word.lower() == "where":
                index_where = i
            i += 1
        output_attributes = []
        for item in split_query[1:index_from]:
            output_attributes.append(item.replace(",",""))
        if output_attributes[0] == "*":
            output_attributes = {}
        # Set mongo collection name
        collection = split_query[index_from + 1].lower()
        mongo_collection = mongo_db[collection]
        if index_where != 0:
            where_conditions = split_query[index_where + 1:]
            # Convert where condition values to correct datatype
            data_type = get_attribute_datatype(mongo_collection, str(where_conditions[0]))
            converted_value = cast_value(data_type, where_conditions[2])
            filter_condition = {'$match':{str(where_conditions[0]):converted_value}}
        else:
            where_conditions = ""
        mongo_output = {"$project":{"_id":0}}
        for item in output_attributes:
            mongo_output["$project"][item] = 1
        # Fetch data
        if where_conditions != "":
            data = mongo_collection.aggregate([filter_condition, mongo_output])
        else:
            data = mongo_collection.aggregate([{'$match': {}}, mongo_output])
        results = list(data)
    except:
        results = -1
    return results

# Function to get results for a query on postgres table
def postgres_query_handler(query):
    try:
        # Fetch data
        cursor = postgres_db.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        # Format results to return with column headings
        column_names = [desc[0] for desc in cursor.description]
        data_frame = pd.DataFrame(data, columns=column_names)
        results = list(data_frame.to_dict(orient="records"))
        cursor.close()
    except:
        postgres_db.rollback()
        results = -1
    return results

# Function to insert data into mongodb table
def mongo_insert_handler(query):
    #try:
        # Split query to seperate collection, attributes and values
        split_query = shlex.split(query)        
        # Set mongo collection name
        collection = split_query[2].lower()
        mongo_collection = mongo_db[collection]
        # Set list of attributes and values to a dictionary
        index_value = 0
        i = 0
        for word in split_query:
            if word.lower() == "values":
                index_value = i
            i += 1
        attributes = []
        for item in split_query[3:index_value]:
            attributes.append(item.replace(",","").replace("(","").replace(")",""))
        values = []
        j = 0
        for item in split_query[index_value + 1:]:
            data_type = get_attribute_datatype(mongo_collection, str(attributes[j]))
            value = item.replace(",","").replace("(","").replace(")","").replace("'","")
            converted_value = cast_value(data_type, value)
            values.append(converted_value)
            j += 1
        record = {attributes[i]: values[i] for i in range(len(attributes))}
        # Insert record
        insert_record = mongo_collection.insert_one(record).inserted_id
        # Fetch the new record
        mongo_output = {"$project":{"_id":0}}
        data = mongo_collection.aggregate([{'$match': {"_id":insert_record}}, mongo_output])
        results = list(data)
    #except:
        #results = -1
        return results

# Function to insert data into postgres table
def postgres_insert_handler(query):
    try:
        # Execute insert query
        cursor = postgres_db.cursor()
        return_query = query + " RETURNING *"
        cursor.execute(return_query)
        if cursor.rowcount == 0:
            print(cursor.rowcount)
            return 0
        # Fetch inserted row
        data = cursor.fetchone()
        postgres_db.commit()
        # Format results to return with column headings
        if data:
            column_names = [desc[0] for desc in cursor.description]
            results = [dict(zip(column_names, data))]
    except:
        postgres_db.rollback()
        results = -1
    cursor.close()
    return results

# Function to update data in mongodb table
def mongo_update_handler(query):
    try:
        # Split query to seperate update statements, collection and where conditions
        split_query = shlex.split(query.replace(","," "))
        index_set = 0
        index_where = 0
        i = 0
        for word in split_query:
            if word.lower() == "set":
                index_set = i
            if word.lower() == "where":
                index_where = i
            i += 1
        update_statements = (split_query[index_set + 1:index_where])
        # Set mongo collection name
        collection = split_query[index_set - 1].lower()
        if collection not in mongo_collections:
            return -1
        mongo_collection = mongo_db[collection]
        if index_where != 0:
            where_conditions = split_query[index_where + 1:]
            # Convert where condition values to correct datatype
            data_type = get_attribute_datatype(mongo_collection, str(where_conditions[0]))
            converted_value = cast_value(data_type, where_conditions[2])
            filter_list = {str(where_conditions[0]):converted_value}
        else:
            filter_list = {}
        # Build a list of update statements
        update_list = []
        j = 0
        for item in update_statements:
            if item == "=":
                update_list.append({"$set":{str(update_statements[j-1]):str(update_statements[j+1])}})
            j += 1
        # Update data
        for update in update_list:
            mongo_collection.update_many(filter_list, update)
        # Fetch updated rows
        mongo_output = {"_id":0}
        data = mongo_collection.find(filter_list, mongo_output)
        results = list(data)
    except:
        results = -1
    return results

# Function to update data in postgres table
def postgres_update_handler(query):
    try:
        # Execute Query
        cursor = postgres_db.cursor()
        cursor.execute(query)
        postgres_db.commit()
        if cursor.rowcount == 0:
            return 0
        # Build the query to fetch updated rows
        query.lower()
        split_query = query.split("where")
        split_parts = split_query[0].split(" ")
        if len(split_query) > 0:
            where_condition = split_query[1]
            fetch_query = "SELECT * FROM " + split_parts[1] + " where" + where_condition + ";"
        else:
            fetch_query = "SELECT * FROM " + split_parts[1] + ";"
        # Fetch updated rows
        cursor.execute(fetch_query)
        data = cursor.fetchall()
        # Format results to return with column headings
        column_names = [desc[0] for desc in cursor.description]
        data_frame = pd.DataFrame(data, columns=column_names)
        results = data_frame.to_dict(orient="records")
    except:
        postgres_db.rollback()
        results = -1
    cursor.close()
    return results

# Function to delete data in mongodb table
def mongo_delete_handler(query):
    try:
        # Split query to seperate collection and where conditions
        split_query = shlex.split(query)
        index_where = 0
        i = 0
        for word in split_query:
            if word.lower() == "where":
                index_where = i
            i += 1
        # Set mongo collection name
        collection = split_query[2].lower()
        mongo_collection = mongo_db[collection]
        if index_where != 0:
            where_conditions = split_query[index_where + 1:]
            # Convert where condition values to correct datatype
            data_type = get_attribute_datatype(mongo_collection, str(where_conditions[0]))
            converted_value = cast_value(data_type, where_conditions[2])
            filter_list = {str(where_conditions[0]):converted_value}
        else:
            filter_list = {}
        print(filter_list)
        # delete records
        result = mongo_collection.delete_many(filter_list)
        results = result.deleted_count
        print(results)
    except:
        results = -1
    return results

# Function to delete data in postgres table
def postgres_delete_handler(query):
    try:
        # Execute delete query
        cursor = postgres_db.cursor()
        cursor.execute(query)
        postgres_db.commit()
        results = cursor.rowcount
    except:
        postgres_db.rollback()
        results = -1
    cursor.close()
    return results

# Function to get the datatype of an attribute from database table
def get_attribute_datatype(table, attribute):
    sample = table.find_one()
    if not sample:
        return
    else:
        sample_data = {key: type(val) for key, val in sample.items()}
        if attribute in sample_data:
            return sample_data[attribute]
        return

# Function to convert a value to the desired datatype
def cast_value(python_type, value):
    if python_type == int:
        return int(value)
    if python_type == float:
        return float(value)
    if python_type == bool:
        return value.lower() == "true"
    return value

# Function to concatenate data from both databases to a single list
def concetenate_data(mongo_data, postgres_data):
    # Extract keys from both lists
    if postgres_data:
        ordered_keys = list(postgres_data[0].keys())
        postgres_keys = set(postgres_data[0].keys())
    else:
        ordered_keys = list(mongo_data[0].keys())
        mongo_keys = set(mongo_data[0].keys())
    # Normalize key order
    def normalize(record):
        return {k: record[k] for k in ordered_keys}
    # Normalize values
    normalized_list1 = []
    for record in mongo_data:
        normalized_record = {col: record.get(col, None) for col in ordered_keys}
        normalized_list1.append(normalized_record)
    normalized_list2 = []
    for record in postgres_data:
        normalized_record = {col: record.get(col, None) for col in ordered_keys}
        normalized_list2.append(normalized_record)
    # Combine lists
    return normalized_list1 + normalized_list2