import mysql.connector
import pandas as pd

def connect_db(database):
    """Connects to the MySQL database."""
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="oodles",
        database=database
    )

def get_parent_entity_id(cursor_read, parent_organisation_id):
    """Fetches the parent entity ID if it exists."""
    if parent_organisation_id is None:
        return None

    cursor_read.execute("""
        SELECT organisation_name, updated_at, created_at
        FROM dmscopy.global_organisations
        WHERE id = %s;
    """, (parent_organisation_id,))
    parent_details = cursor_read.fetchone()

    if not parent_details:
        return None  # Parent organisation not found

    name, updated_at, created_at = parent_details

    cursor_read.execute("""
        SELECT entity_id FROM entitysample.entity
        WHERE name = %s AND updated_at = %s AND created_at = %s;
    """, (name, updated_at, created_at))

    result = cursor_read.fetchone()
    return result[0] if result else None

def process_record(record_id):
    """Processes a single organisation record and inserts data into entity and entity_property."""
    conn_db1 = connect_db("dmscopy")
    conn_db2 = connect_db("entitysample")

    # Use buffered cursors to avoid "Unread result found" error
    cursor_read_db1 = conn_db1.cursor(buffered=True)
    cursor_read_db2 = conn_db2.cursor(buffered=True)
    cursor_write_db2 = conn_db2.cursor(buffered=True)

    # Fetch organisation details
    cursor_read_db1.execute("""
        SELECT creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, 
               organisation_name, parent_organisation_id, trade_name 
        FROM dmscopy.global_organisations
        WHERE id = %s AND parent_organisation_id IS NOT NULL;
    """, (record_id,))
    record = cursor_read_db1.fetchone()

    if not record:
        cursor_read_db1.close()
        cursor_read_db2.close()
        cursor_write_db2.close()
        conn_db1.close()
        conn_db2.close()
        return  # Skip if no record found

    creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, organisation_name, parent_organisation_id, trade_name = record

    # Get parent entity ID if applicable
    parent_entity_id = get_parent_entity_id(cursor_read_db2, parent_organisation_id)

    # Insert into entity table
    insert_entity_query = """
        INSERT INTO entitysample.entity 
        (name, creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, type, parent_entity_id, trade_name)
        VALUES (%s, %s, %s, %s, %s, %s, %s, 1, %s, %s);
    """
    cursor_write_db2.execute(insert_entity_query, (organisation_name, creator_ledger_id, created_by, updated_by, created_at, updated_at, deleted_at, parent_entity_id, trade_name))
    entity_last = cursor_write_db2.lastrowid  # Get inserted entity ID

    # Commit entity insertion
    conn_db2.commit()

    # Define properties to insert
    properties = [
        "fy_start_month", "fy_end_month", "website", "registration_number", "vat_number", "registered_country_id",
        "logo_url", "currency_id", "description", "nace_section_id", "revenue_access_number", "hierarchy_code",
        "CRM_ID", "exchange_ref_id", "is_delete", "leads_transactions_id", "status_id", "industry_id", "trade_name"
    ]

    for prop in properties:
        # Check if property exists in the property table
        cursor_read_db2.execute("SELECT property_id FROM entitysample.property WHERE property_id = %s", (prop,))
        if cursor_read_db2.fetchone():  # Ensures the result is fully consumed
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
            cursor_write_db2.execute(insert_property_query, (entity_last, record_id))

    # Commit property insertions
    conn_db2.commit()

    # Close connections
    cursor_read_db1.close()
    cursor_read_db2.close()
    cursor_write_db2.close()
    conn_db1.close()
    conn_db2.close()

# Fetch all records and process them
conn_db1 = connect_db("dmscopy")
df = pd.read_sql("SELECT id FROM global_organisations", conn_db1)
conn_db1.close()

for record_id in df["id"]:
    process_record(record_id)

print("Data transferred successfully.")
