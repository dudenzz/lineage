from mssql_python import connect
from mssql_python.connection import Connection
from config import db_database, db_hostname, db_directory, db_service, db_creation_script_url
from queries import drop_database, create_database, create_lineage_structure, scenarios, get_all_tables, get_all_views
import subprocess
import requests


def grant_permissions_to_database_directory():
    folder_path = db_directory
    service_account = f"NT SERVICE\\{db_service}"
    command = f'icacls "{folder_path}" /grant "{service_account}:(OI)(CI)F" /T /C'
    try:
        subprocess.run(command, shell=True, check=True)
        print(f"Permissions granted to {service_account} on {folder_path}")
    except subprocess.CalledProcessError as e:
        print("Error setting permissions:", e)

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()
def execute_sql_script(connection, script):
    statements = [s.strip() for s in script.splitlines()]
    batch = ""
    cursor = connection.cursor()
    cursor.execute(f"USE {db_database}")
    for line in statements:
        if line.upper() == "GO":
            if batch.strip():
                try:
                    cursor.execute(batch)
                    connection.commit()
                except Exception as e:
                    print("Error executing batch:", e)
                    raise
                batch = ""
        else:
            batch += line + "\n"

    if batch.strip():
        try:
            cursor.execute(batch)
            connection.commit()
        except Exception as e:
            print("Error executing batch:", e)
            raise

    cursor.close()
    print("Script executed successfully!")
def execute_sql_script_from_url(connection, script_url):
    print("Downloading the script...")
    script = requests.get(script_url).text 
    execute_sql_script(connection, script)
def clear_lineage_structures(connection):
    execute_sql_script(connection, create_lineage_structure)
def clean_database(connection):
    print("Switching to master")
    execute_query(connection,"USE master")
    print("Deleting Northind database")
    execute_query(connection,drop_database)
    print("Creating new database")
    execute_query(connection,create_database)
    print("Switching to a new databse")
    execute_query(connection,f"USE {db_database}")
    print("Creating a fresh Northwind schema and filling it with data")
    execute_sql_script_from_url(connection,db_creation_script_url)
    print("Creating the lineage structure")


def create_connection():
    connection_string = (
    f"SERVER={db_hostname};"
    f"DATABASE=master;"
    "Trusted_Connection=yes;"
    "Encrypt=yes;"
    "TrustServerCertificate=yes;"
    )
    conn = connect(connection_string)
    conn.autocommit = True
    return conn


def create_scenario(connection, type, number):
    scenario = scenarios[type][number-1]['script']
    print(f'Addding a {type} scenario number {number}')
    execute_sql_script(connection, scenario)
    
def get_all_tables_info(connection : Connection):
    cursor = connection.cursor()
    cursor.execute(f"USE {db_database}")
    tables = cursor.execute(get_all_tables)
    for table in tables:
        yield table

def get_all_views_info(connection : Connection):
    cursor = connection.cursor()
    cursor.execute(f"USE {db_database}")
    tables = cursor.execute(get_all_views)
    for table in tables:
        yield table

def get_all_data(connection : Connection, object_name ):
    cursor = connection.cursor()
    cursor.execute(f"USE {db_database}")
    data = cursor.execute(f"SELECT * FROM {object_name}")
    description = cursor.description
    column_names = tuple(col[0] for col in description)
    column_types = tuple(col[1] for col in description)
    yield column_names
    yield column_types
    for entry in data:
        yield entry



def create_all_scenarios():
    grant_permissions_to_database_directory()
    conn = create_connection()
    clean_database(conn)
    clear_lineage_structures(conn)
    create_scenario(conn,'select',1)
    create_scenario(conn,'select',2)
    create_scenario(conn,'select',3)
    create_scenario(conn,'select',4)
    create_scenario(conn,'select',5)
    create_scenario(conn,'join',1)
    create_scenario(conn,'join',2)
    create_scenario(conn,'join',3)
    create_scenario(conn,'join',4)
    create_scenario(conn,'join',5)
    create_scenario(conn,'transformation',1)
    create_scenario(conn,'transformation',2)
    create_scenario(conn,'transformation',3)
    create_scenario(conn,'transformation',4)
    create_scenario(conn,'transformation',5)
    conn.close()


def main():
    conn = create_connection()
    for entry in get_all_data(conn, "vw_EmployeeBasicInfo"):
        print(entry)

    conn.close()

if __name__ == "__main__":
    main()