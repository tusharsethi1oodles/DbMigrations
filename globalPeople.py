import mysql.connector
import pandas as pd

def connect_db(database):
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database=database
    )

def process_record(record_id):
    conn_db1 = connect_db("dmscopy")
    conn_db2 = connect_db("entitysample")
    cursor_db1 = conn_db1.cursor()
    cursor_db2 = conn_db2.cursor()

    try:
        insert_entity_query = """
        INSERT INTO entitysample.entity (creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, type, name)
        SELECT 
            creator_ledger_id, 
            created_by, 
            updated_by, 
            created_at, 
            updated_at, 
            deleted_at, 
            2, 
            CONCAT(
                COALESCE(first_name, ''),  
                CASE 
                    WHEN COALESCE(first_name, '') != '' AND COALESCE(last_name, '') != '' THEN ' '  
                    ELSE ''  
                END,  
                COALESCE(last_name, '')
            )
        FROM dmscopy.global_people
        WHERE id = %s
        AND NOT (first_name IS NULL AND last_name IS NULL);
        """
        
        cursor_db2.execute(insert_entity_query, (record_id,))
        entity_last = cursor_db2.lastrowid  
        conn_db2.commit() 
        print(f"Inserted entity_id: {entity_last} ")  # Debugging

        if not entity_last:
            print(f"Skipping record {record_id} as entity both first and last name are NULL")
            return 

        insert_people_query = """
        INSERT INTO entitysample.people (entity_id, salutation, first_name, last_name, title, date_of_birth, created_by, updated_by, created_at, updated_at, type)
        SELECT 
            %s, 
            salutation, 
            first_name, 
            last_name, 
            title, 
            CASE 
                WHEN date_of_birth LIKE '%/%' THEN STR_TO_DATE(date_of_birth, '%d/%m/%Y') 
                ELSE date_of_birth 
            END, 
            created_by, 
            updated_by, 
            created_at, 
            updated_at,
            2
        FROM dmscopy.global_people
        WHERE id = %s;
        """
        try:
            cursor_db2.execute(insert_people_query, (entity_last, record_id))
            conn_db2.commit()
        except mysql.connector.Error as err:
            print(f"Error inserting into people for record {record_id}: {err}")
            return  

        # Insert into entity_property for multiple properties
        properties = [
            "former_last_name", "notes", "ppsn_document_type", "photo_url", "pronounced", "signature_attachment",
            "CRM_ID", "exchange_ref_id", "is_delete", "import_people_name", "leads_transactions_id", "status_id", "industry_id"
        ]
        
        for prop in properties:
            try:
                cursor_db2.execute("SELECT property_id FROM entitysample.property WHERE property_id = %s", (prop,))
                if cursor_db2.fetchone():
                    condition = f"{prop} IS NOT NULL"
                    if prop == "leads_transactions_id":
                        condition += " AND leads_transactions_id != 0"
                    if prop == "is_delete":
                        condition += " AND is_delete != 0"
                    elif prop == "pronounced":
                        condition += " AND pronounced != ''"

                    insert_property_query = f"""
                    INSERT INTO entitysample.entity_property (entity_id, property_id, property_value)
                    SELECT %s, '{prop}', {prop}
                    FROM dmscopy.global_people
                    WHERE id = %s AND {condition};
                    """
                    cursor_db2.execute(insert_property_query, (entity_last, record_id))
                    conn_db2.commit()
            except mysql.connector.Error as err:
                print(f"Error inserting {prop} for record {record_id}: {err}")

    except mysql.connector.Error as err:
        print(f"Error processing record {record_id}: {err}")

    finally:
        cursor_db1.close()
        cursor_db2.close()
        conn_db1.close()
        conn_db2.close()

# Fetch all records and process them
conn_db1 = connect_db("dmscopy")
df = pd.read_sql("SELECT id FROM global_people", conn_db1)
conn_db1.close()

for record_id in df["id"]:
    process_record(record_id)
