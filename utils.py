import os
import sys
import json
import inspect
import psycopg2
import subprocess
import shlex
from psycopg2 import sql
from datetime import datetime, timezone
from pathlib import Path
import base64
from cryptography.fernet import Fernet

# methods related to logging
def setup_loggers():
    log_dir = Path.cwd() / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    today_date = datetime.now(timezone.utc).strftime("%d-%m-%Y")
    info_log_file = log_dir / f"table_creation_{today_date}_info.log"
    error_log_file = log_dir / f"table_creation_{today_date}_error.log"

    return info_log_file, error_log_file


def log_message(message, log_file, level="INFO"):
    """Logs a message with a specified log level (INFO, ERROR, etc.)."""
    log_file.touch(exist_ok=True)  

    caller_line_number = inspect.stack()[1].lineno
    timestamp = datetime.now(timezone.utc).strftime("%H:%M:%S")
    if level=="INFO":
        log_entry = f"({level},{timestamp} :{message})\n"
    else:
        log_entry = f"line {caller_line_number} - {level}, ({timestamp} :{message})\n"

    with log_file.open("a") as file:
        file.write(log_entry)

# method to generate report
def generate_report(server_name, report):
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)

    current_datetime = datetime.now(timezone.utc).strftime("%d-%m-%Y_%H-%M-%S")
    report_filename = f"{server_name}-{current_datetime}.json"
    report_path = reports_dir / report_filename

    try:
        report_path.write_text(json.dumps(report, indent=4))
        print(
            f"------------------------- Report saved to {report_path} -------------------------\n"
        )
    except IOError as e:
        print(f"Error writing report to file: {e}")

    return report_path

# methods for encrypting and decrypting the password
def command_run(cmd, verbose=False):
    try:
        cmd = shlex.split(cmd)
        process = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, shell=False
        )
        std_out, std_err = process.communicate()
        if verbose:
            print(std_out.strip(), std_err)
        return std_out
    except Exception as e:
        message = e
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return None


def get_key():
    try:
        if "nt" in os.name:
            key = command_run("wmic csproduct get uuid")
        else:
            key = command_run("cat /etc/machine-id")

        key = key[:32]
        key = key.encode()
        key = base64.urlsafe_b64encode(key)
        return key
    except Exception as e:
        message = f"Error getting key: {str(e)}"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return None


def psswrd_encrypt(password):
    """
    Encrypts the password and returns it as a Base64-encoded string.
    """
    if not password:
        message = "Error: Password is empty"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return None

    try:
        key = get_key()
        fernet = Fernet(key)
        encrypted_password = fernet.encrypt(password.encode())
        return base64.urlsafe_b64encode(encrypted_password).decode("utf-8")
    except Exception as e:
        message = f"Error encrypting password: {str(e)}"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return None


def psswrd_decrypt(encrypted_psswrd):
    """
    Decrypts a Base64-encoded encrypted password.
    """
    if not encrypted_psswrd:
        message = "Error: No encrypted password provided."
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return None

    try:
        key = get_key()
        fernet = Fernet(key)
        encrypted_psswrd_bytes = base64.urlsafe_b64decode(encrypted_psswrd)
        decrypted_psswrd = fernet.decrypt(encrypted_psswrd_bytes)
        return decrypted_psswrd.decode("utf-8")
    except Exception as e:
        message = f"An error occurred during decryption: {str(e)}"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return None


# method to load the config file
def load_config(config_path="config.json"):
    """Loads the configuration from a JSON file."""
    config_path = Path(config_path)

    if not config_path.exists():
        message = f"Error: Configuration file '{config_path}' not found."
        log_message(message, error_log_file, level="ERROR")
        print(message)
        sys.exit(1)

    try:
        return json.loads(config_path.read_text())
    except json.JSONDecodeError as e:
        message = (
            f"Error: Failed to decode JSON in configuration file '{config_path}': {e}"
        )
        log_message(message, error_log_file, level="ERROR")
        print(message)
        sys.exit(1)


#  methods to create and close podtgres connection
def create_postgres_connection(config):
    """Creates and returns a PostgreSQL database connection."""
    log_message("Connecting to PostgreSQL...", info_log_file)
    try:
        conn = psycopg2.connect(
            dbname=config["postgres_conn"]["db_name"],
            user=config["postgres_conn"]["user"],
            password=psswrd_decrypt(config["postgres_conn"]["password"]),
            host=config["postgres_conn"]["host"],
            port=config["postgres_conn"]["port"],
        )
        log_message("Connection successful!", info_log_file)
        return conn
    except (psycopg2.OperationalError, psycopg2.Error) as e:
        message = f"Database connection error: {e}"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return False


def close_connection(conn):
    """Closes the PostgreSQL database connection."""
    if conn:
        try:
            conn.close()
            log_message("Connection closed.", info_log_file)
        except psycopg2.Error as e:
            message = f"Error closing the connection: {e}"
            log_message(message, error_log_file, level="ERROR")
            print(message)
            sys.exit(1)

# executing the query
def execute_query(conn, query):
    """Executes a SQL query and returns results if applicable."""
    try:
        with conn.cursor() as cur:
            log_message(f"Executing query: \n{query}\n", info_log_file)
            cur.execute(sql.SQL(query))
            if query.strip().lower().startswith("select"):
                return cur.fetchall()
            conn.commit()
    except psycopg2.Error as e:
        message = f"Error executing query: {e}"
        log_message(
            message,
            error_log_file,
            level="ERROR",
        )
        print(message)
        raise


def replace_foreign_table(conn, drop_query, create_query):
    """Drops and recreates a foreign table in a single transaction."""
    try:
        with conn.cursor() as cur:
            log_message(f"Executing query: \n{drop_query}\n", info_log_file)
            cur.execute(sql.SQL(drop_query))

            log_message(f"Executing query: \n{create_query}\n", info_log_file)
            cur.execute(sql.SQL(create_query))

        conn.commit()

    except psycopg2.Error as e:
        conn.rollback()

        message = f"Error replacing foreign table: {e}"
        log_message(
            message,
            error_log_file,
            level="ERROR",
        )
        print(message)
        raise

info_log_file, error_log_file = setup_loggers()

# method to check wether the foreign server exists or not
def foreign_server_exists(conn, server_name):

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_foreign_server
                    WHERE srvname = %s
                );
            """,
                (server_name,),
            )
            exists = cursor.fetchone()[0]
            return exists
    except Exception as e:
        print(f"An error occurred: {e}")
        return False

# method to rename the spaced folder to %20
def rename_folders_with_spaces(base_dir):
    dir_path = Path(base_dir)

    for dirpath in dir_path.rglob("*"):
        if dirpath.is_dir() and " " in dirpath.name:
            new_dirname = dirpath.name.replace(" ", "%20")
            new_dir_path = dirpath.parent / new_dirname

            dirpath.rename(new_dir_path)

