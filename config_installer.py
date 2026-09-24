import json
import getpass
from pathlib import Path
import sys
from utils import create_postgres_connection, close_connection, psswrd_encrypt, foreign_server_exists

config = {
    "commands": {
        "drop_table": 'DROP FOREIGN TABLE IF EXISTS "{schema_name}"."{table}";',
        "create_schema": 'CREATE SCHEMA IF NOT EXISTS "{name}";',
        "foreign_table": 'CREATE FOREIGN TABLE IF NOT EXISTS "{schema}"."{table}" ({columns}) SERVER "{server}" OPTIONS (filename \'{file_path}\' );',
        "select": 'SELECT count(*) FROM "{schema_name}"."{table_name}";',
    },
    "table_creation": {
        "replace_existing": False,
        "verify_row_count": False
    },
    "postgresDatatypes": {
        "BinaryType": "BYTEA",
        "StringType": "TEXT",
        "DoubleType": "DOUBLE PRECISION",
        "DateType": "DATE",
        "TimestampType": "TIMESTAMP",
        "IntegerType": "INTEGER",
        "FloatType": "REAL",
        "ShortType": "SMALLINT"
    },
}


def get_non_empty_input(prompt):
    """Prompt user for input until a non-empty value is provided."""
    while True:
        value = input(prompt).strip()
        if value:
            return value
        print("Input cannot be empty. Please try again.")


def get_config_details():
    """Prompt user for PostgreSQL connection details and server name then save them if confirmed."""
    print("\nPlease enter the postgres connection details")
    postgres_conn = {
        "host": get_non_empty_input("Hostname / IP address: "),
        "port": input("Port number [default: 5432]: ") or "5432",
        "db_name": get_non_empty_input("Database name: "),
        "user": get_non_empty_input("Username: "),
        "password": psswrd_encrypt(getpass.getpass("Password for user: ")),
    }

    config["postgres_conn"] = postgres_conn

    print("\nTesting connection...")
    conn = create_postgres_connection(config)

    if conn != False:
        print("Connection successful")
        
        while True:
            server_name = get_non_empty_input("\nEnter the foreign server name: ").strip().lower()
            config["server_name"] = server_name
            server_exists = foreign_server_exists(conn, server_name)
            
            if server_exists:
                print("\nPlease confirm the following details:\n")
                print(
                    f"PostgreSQL Connection Details \nDB Name: {postgres_conn['db_name']} \nUser: {postgres_conn['user']} \nHost: {postgres_conn['host']} \nPort: {postgres_conn['port']} \nForeign Server Name: {server_name}"
                )
                break
            else:
                print(f"\nThe provided server name does not exist: {server_name}")
                retry = input("Would you like to try again? (y/n)[default y]: ").strip().lower() or 'y'
                if retry != 'y':
                    print("Exiting.")
                    sys.exit()


        save_flag = input("\nProceed to save? (y/n)[default y]: ") or 'y'

        if save_flag == "y":
            config_file_path = Path("config.json")
            if config_file_path.exists():
                replace_flag = input(
                    f"\n{config_file_path} file already exists. Do you want to replace it? (y/n)[default y]: "
                ).lower() or "y"
                if replace_flag != "y":
                    print("\nCredentials not saved.\n")
                    sys.exit()

            with open(config_file_path, "w") as config_file:
                json.dump(config, config_file, indent=4)
            print("\nCredentials saved successfully to config.json.\n")
        else:
            print("\nCredentials not saved. \n")
            sys.exit()

        close_connection(conn)
    else:
        retry_flag = (input("Connection refused, do you want to try again?\n (y/n)[default y]") or "y").lower()
        if retry_flag == "y":
            get_config_details()
        else:
            exit()
    

get_config_details()
