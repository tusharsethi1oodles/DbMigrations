import pandas as pd
import numpy as np
import mysql.connector

cnt_not_present=0
cnt_isNull=0

def connect_db(database):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database=database
    )

def get_entity_details(dms_entity_id,entity_type,connection):
    cursor=connection.cursor(buffered=True)
    
    if(entity_type == 1):
        search_in_global_organisation = """ 
            SELECT organisation_name AS full_name, created_at ,updated_at
            FROM dmscopy.global_organisations
            WHERE id = %s
            """
        cursor.execute(search_in_global_organisation,(dms_entity_id,))
        result = cursor.fetchone()

        if result:
            full_name, created_at ,updated_at= result
            search_in_entity = """ 
                SELECT entity_id 
                FROM entity 
                WHERE name = %s AND created_at = %s AND updated_at=%s
                """
            cursor.execute(search_in_entity, (full_name, created_at,updated_at))
            entity_result = cursor.fetchone()

            if entity_result:
                return entity_result[0]
        else:
            return None
    elif (entity_type == 2):
        search_in_global_people = """ 
            SELECT CONCAT(first_name, ' ', last_name) AS full_name, created_at ,updated_at
            FROM dmscopy.global_people 
            WHERE id = %s
            """
        cursor.execute(search_in_global_people, (dms_entity_id,))
        result = cursor.fetchone()

        if result:
            full_name, created_at ,updated_at= result

            search_in_entity = """ 
            SELECT entity_id 
            FROM entity 
            WHERE name = %s AND created_at = %s AND updated_at=%s
            """
            cursor.execute(search_in_entity, (full_name, created_at,updated_at))
            entity_result = cursor.fetchone()

            if entity_result:
                return entity_result[0] 

        return None 
    else:
        print('entity type should be one or two ')
        return None
    
def get_country_code(dms_country_id, db1_cursor, db2_cursor):
    """
    Retrieves the country code from global_countries and checks if it exists in param_country.

    Parameters:
    - dms_country_id: ID of the country in global_countries.
    - db1_cursor: Cursor for the first database connection.
    - db2_cursor: Cursor for the second database connection.

    Returns:
    - country_code (str) if found in param_country, otherwise None.
    """
    try:
        if not dms_country_id:
            return None  # No country ID provided

        # Query to get the country code
        search_country_code_query = """
            SELECT iso_code_alpha3 FROM global_countries 
            WHERE id = %s
        """
        db1_cursor.execute(search_country_code_query, (dms_country_id,))
        result = db1_cursor.fetchone()

        if not result or not result[0]:  # Check if a valid result was found
            return None
        
        country_code = result[0]  # Extract the actual string value

        # Query to check if country exists in param_country
        search_code_in_param_country_query = """
            SELECT COUNT(*) FROM param_country WHERE country_id = %s;
        """
        db2_cursor.execute(search_code_in_param_country_query, (country_code,))
        count = db2_cursor.fetchone()[0]  # Extract count value

        return country_code if count >= 1 else None

    except Exception as e:
        print(f"Error: {e}")  # Better error handling
        return None  # Return None in case of an error

def process_record(dms_row):
    conn_db1 = connect_db("dmscopy")
    conn_db2 = connect_db("entitysample")

    dms_entity_id, entity_type = dms_row['entity_id'], dms_row['entity_type']
    
    if dms_entity_id is None:
        print('dms_entity_id is NULL..')
        global cnt_isNull
        cnt_isNull +=1
        return

    db1_cursor = conn_db1.cursor()
    db2_cursor = conn_db2.cursor()

    # entity_id = get_entity_id(people_id, conn_db2)
    entities_db_entity_id=get_entity_details(dms_entity_id,entity_type,conn_db2)
    
    dms_address_1 = dms_row.get('address_1') or ""
    dms_address_2 = dms_row['address_2']
    dms_address_3 = dms_row['address_3']
    dms_city = dms_row['city']
    dms_state_county = dms_row['state_county']
    dms_postal_code = dms_row['postal_code']
    dms_country_id = dms_row['country_id']
    dms_address_type=dms_row['address_type']

    
    country_code=get_country_code(dms_country_id, db1_cursor, db2_cursor)


    if entities_db_entity_id:
        insert_into_entity_address = """ 
        INSERT INTO address (entity_id, line_one, line_two, area,city,
        state,zipcode,country,country_code,address_type) 
        VALUES (%s,%s, %s,%s,%s,%s,%s,%s,%s,%s)
        """
        db2_cursor.execute(insert_into_entity_address, 
                           (entities_db_entity_id, dms_address_1, dms_address_2,
                            dms_address_3,dms_city,dms_state_county,
                            dms_postal_code,dms_state_county,country_code,dms_address_type))
        
        conn_db2.commit()  # Commit the transaction
    else:
        print(f'No entity id present for dms_entity_id {dms_entity_id}')
        global cnt_not_present
        cnt_not_present+=1

    db1_cursor.close()
    db2_cursor.close()
    conn_db1.close()
    conn_db2.close()


# Read data from MySQL
conn_db1 = connect_db("dmscopy")
df = pd.read_sql("SELECT entity_id, entity_type, address_1, address_2,address_3, city, state_county,  country_id, postal_code,address_type FROM dmscopy.addresses", conn_db1)
conn_db1.close()

# Process each record
for index, row in df.iterrows():
    clean_row = row.replace({np.nan: None, "": None}).to_dict()  # Convert NaN & empty strings to None
    process_record(clean_row)


print(f'addresses entity_id not present count is {cnt_not_present}')
print(f'addresses entity_id NULL count is {cnt_isNull}')
