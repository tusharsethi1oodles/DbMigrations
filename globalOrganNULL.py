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
    
    # Insert into entity and get the inserted entity_id
    insert_entity_query = """
    INSERT INTO entitysample.entity (creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, type, name,trade_name)
    SELECT 
        creator_ledger_id, 
        created_by, 
        updated_by, 
        created_at, 
        updated_at, 
        deleted_at, 
        1, 
        organisation_name,
        trade_name
    FROM dmscopy.global_organisations
    WHERE id = %s AND parent_organisation_id IS NULL;
    """
    cursor_db2.execute(insert_entity_query, (record_id,))
    entity_last = cursor_db2.lastrowid 
    conn_db2.commit()  # Commit to save changes

    properties = [
        "fy_start_month", "fy_end_month", "website", "registration_number", "vat_number", "registered_country_id",
        "logo_url", "currency_id", "description", "nace_section_id", "revenue_access_number", "hierarchy_code",
        "CRM_ID", "exchange_ref_id", "is_delete", "leads_transactions_id", "status_id", "industry_id", "trade_name"
    ]

    for prop in properties:
        # Check if property exists in the property table
        cursor_db2.execute("SELECT property_id FROM entitysample.property WHERE property_id = %s", (prop,))
        if cursor_db2.fetchone():  # Ensures the result is fully consumed
            condition = f"{prop} IS NOT NULL"
            if prop in ["website", "registration_number", "vat_number", "description", "revenue_access_number", "CRM_ID", "leads_transactions_id", "trade_name"]:
                condition += f" AND {prop} != ''"
            elif prop == "leads_transactions_id":
                condition += " AND leads_transactions_id != 0"
            elif prop == "is_delete":
                condition += " AND is_delete != 0"

            # Insert property value into entity_property
            insert_property_query = f"""
            INSERT INTO entitysample.entity_property (entity_id, property_id, property_value)
            SELECT %s, '{prop}', {prop}
            FROM dmscopy.global_organisations
            WHERE id = %s AND {condition};
            """
            cursor_db2.execute(insert_property_query, (entity_last, record_id))

    # Commit property insertions
    conn_db2.commit()
    
    # Close connections
    cursor_db1.close()
    cursor_db2.close()
    conn_db1.close()
    conn_db2.close()

# Fetch all records where parent_organisation_id is NULL and process them
conn_db1 = connect_db("dmscopy")
df = pd.read_sql("SELECT id FROM global_organisations WHERE parent_organisation_id IS NULL", conn_db1)
conn_db1.close()

for record_id in df["id"]:
    process_record(record_id)

print("Data transferred successfully.")

