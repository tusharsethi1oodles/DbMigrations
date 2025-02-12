import pandas as pd
import mysql.connector

def connect_db(database):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database=database
    )

def get_entity_id(organisation_id, connection):
    cursor = connection.cursor(buffered=True)

    # Query to get full_name and created_at from global_organisation
    search_in_global_organisation = """ 
    SELECT organisation_name AS full_name, created_at 
    FROM dmscopy.global_organisations
    WHERE id = %s
    """
    cursor.execute(search_in_global_organisation, (organisation_id,))
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
            return entity_result[0]  

    return None  


def process_record(organisation_id, crm_id):
    conn_db1 = connect_db("dmscopy")
    conn_db2 = connect_db("entitysample")

    db1_cursor = conn_db1.cursor()
    db2_cursor = conn_db2.cursor()

    entity_id = get_entity_id(organisation_id, conn_db2)

    if entity_id:
        insert_into_entity_property = """ 
        INSERT INTO entity_property (entity_id, property_id, property_value) 
        VALUES (%s, %s, %s)
        """
        db2_cursor.execute(insert_into_entity_property, (entity_id, 'organisation_crm_ids', crm_id))
        conn_db2.commit()  # Commit the transaction

    db1_cursor.close()
    db2_cursor.close()
    conn_db1.close()
    conn_db2.close()


# Read data from MySQL
conn_db1 = connect_db("dmscopy")
df = pd.read_sql("SELECT organisation_id, crm_id FROM organisation_crm_ids", conn_db1)
conn_db1.close()

# Process each record
for index, row in df.iterrows():
    process_record(row["organisation_id"], row["crm_id"])
