# import pandas as pd
# import mysql.connector

# def connect_db(database):
#     return mysql.connector.connect(
#         host="localhost",
#         user="root",
#         password="oodles",
#         database=database
#     )


# def get_entity_id(people_id, connection):
#     cursor = connection.cursor()

#     # Query to get full_name and created_at from global_people
#     search_in_global_people = """ 
#     SELECT CONCAT(first_name, ' ', last_name) AS full_name, created_at 
#     FROM dmscopy.global_people 
#     WHERE id = %s
#     """
#     cursor.execute(search_in_global_people, (people_id,))
#     result = cursor.fetchone()

#     if result:
#         full_name, created_at = result

#         # Query to search in entity table
#         search_in_entity = """ 
#         SELECT entity_id 
#         FROM entity 
#         WHERE name = %s AND created_at = %s
#         """
#         cursor.execute(search_in_entity, (full_name, created_at))
#         entity_result = cursor.fetchone()

#         if entity_result:
#             return entity_result[0]  # Returning entity_id

#     return None  # Return None if no match found


# def process_record(people_id, crm_id):
#     conn_db1 = connect_db("dmscopy")
#     conn_db2 = connect_db("entitysample")

#     db1_cursor = conn_db1.cursor()
#     db2_cursor = conn_db2.cursor()

#     entity_id = get_entity_id(people_id, conn_db2)

#     if entity_id:
#         insert_into_entity_property = """ 
#         INSERT INTO entity_property (entity_id, property_id, property_value) 
#         VALUES (%s, %s, %s)
#         """
#         db2_cursor.execute(insert_into_entity_property, (entity_id, 'people_crm_ids', crm_id))
#         conn_db2.commit()  # Commit the transaction

#     db1_cursor.close()
#     db2_cursor.close()
#     conn_db1.close()
#     conn_db2.close()

# conn_db1=connect_db("dmscopy")
# df=pd.read_sql("SELECT people_id,crm_id from people_crm_ids")
# conn_db1.close()

# for people_id,crm_id in df:


import pandas as pd
import mysql.connector

def connect_db(database):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database=database
    )

def get_entity_id(people_id, connection):
    cursor = connection.cursor(buffered=True)

    # Query to get full_name and created_at from global_people
    search_in_global_people = """ 
    SELECT CONCAT(first_name, ' ', last_name) AS full_name, created_at 
    FROM dmscopy.global_people 
    WHERE id = %s
    """
    cursor.execute(search_in_global_people, (people_id,))
    result = cursor.fetchone()

    if result:
        full_name, created_at = result

        search_in_entity = """ 
        SELECT entity_id 
        FROM entity 
        WHERE name = %s AND created_at = %s
        """
        cursor.execute(search_in_entity, (full_name, created_at))
        entity_result = cursor.fetchone()

        if entity_result:
            return entity_result[0]  # Returning entity_id

    return None  # Return None if no match found


def process_record(people_id, crm_id):
    conn_db1 = connect_db("dmscopy")
    conn_db2 = connect_db("entitysample")

    db1_cursor = conn_db1.cursor()
    db2_cursor = conn_db2.cursor()

    entity_id = get_entity_id(people_id, conn_db2)

    if entity_id:
        insert_into_entity_property = """ 
        INSERT INTO entity_property (entity_id, property_id, property_value) 
        VALUES (%s, %s, %s)
        """
        db2_cursor.execute(insert_into_entity_property, (entity_id, 'people_crm_ids', crm_id))
        conn_db2.commit()  # Commit the transaction

    db1_cursor.close()
    db2_cursor.close()
    conn_db1.close()
    conn_db2.close()


# Read data from MySQL
conn_db1 = connect_db("dmscopy")
df = pd.read_sql("SELECT people_id, crm_id FROM people_crm_ids", conn_db1)
conn_db1.close()

# Process each record
for index, row in df.iterrows():
    process_record(row["people_id"], row["crm_id"])
