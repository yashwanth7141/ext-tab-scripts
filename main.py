import pathlib
from pathlib import Path
from datetime import datetime
import sys
from pyspark.sql import SparkSession

from utils import (
    create_postgres_connection,
    load_config,
    close_connection,
    generate_report,
    setup_loggers,
    log_message,
    execute_query,
    foreign_server_exists,
    replace_foreign_table,
)

# Setting up logging and initialize report dictionary
info_log_file, error_log_file = setup_loggers()
report = {}


def retrieve_parquet_files(parquet_path):
    """Retrieve Parquet file paths from the given path."""
    parquet_path = Path(parquet_path)

    try:
        if parquet_path.is_file() and parquet_path.suffix == ".parquet":
            return str(parquet_path), 1

        if parquet_path.is_dir():
            parquet_files = [
                str(file)
                for file in parquet_path.rglob("*.parquet")
                if file.is_file() and not file.name.startswith(".")
            ]

            if parquet_files:
                return " ".join(parquet_files), len(parquet_files)

            message = (
                f"Issue: No Parquet files found in the directory {parquet_path}"
            )
            log_message(message, error_log_file, level="ERROR")
            print(message)
            return None

    except Exception as e:
        message = f"Error retrieving Parquet files from {parquet_path}: {e}"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return None

    return None 


def spark_to_sql_type(spark_type, config):
    """Maps Spark SQL types to PostgreSQL types."""
    types = config.get("postgresDatatypes", {}).get(
        spark_type.__class__.__name__, "TEXT"
    )
    return types


def get_parquet_schema(spark, parquet_path, config, merge_schemas=False):
    """Extracts column names and data types from Parquet files using PySpark."""
    try:
        columns_to_exclude = ["ArchiveCreationDate"]
        log_message("Extracting schema from Parquet file(s)...", info_log_file)

        read_option = (
            spark.read.option("mergeSchema", "true") 
            if merge_schemas 
            else spark.read
        )

        df = read_option.parquet(parquet_path)

        filtered_fields = [
            field 
            for field in df.schema.fields 
            if field.name not in columns_to_exclude
        ]

        schema_extracted = ",".join(
            [
                f"\n\t{field.name} {spark_to_sql_type(field.dataType, config)}"
                for field in filtered_fields
            ]
        )
        log_message(f"Schema extracted: {schema_extracted}", info_log_file)

        return schema_extracted

    except Exception as e:
        message = f"Error extracting schema: {e}"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return None  


def validate_folder(base_path):
    base_path = pathlib.Path(base_path)

    if not base_path.exists():
        message = f"Path {base_path} not found"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return False

    parquet_dir = base_path / "parquet"
    if not parquet_dir.is_dir():
        message = f"Parquet directory not found: {parquet_dir}"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        return False

    parquet_subdirs = [
        subdir
        for subdir in parquet_dir.iterdir()
        if subdir.is_dir() and subdir.name.endswith(".parquet")
    ]

    return len(parquet_subdirs) > 0


def discover_schema_paths(root_path):
    """
    Discovers valid schema paths under the given root path.

    Supported structures:

    1. Single schema:
        root/
        └── parquet/
            └── TABLE.parquet/

    2. Multiple schemas:
        root/
        ├── schema1/
        │   └── parquet/
        │       └── TABLE.parquet/
        ├── schema2/
        │   └── parquet/
        │       └── TABLE.parquet/
        └── schema3/
            └── parquet/
                └── TABLE.parquet/

    3. Mixed structure:
        root/
        ├── parquet/
        │   └── TABLE.parquet/
        ├── schema1/
        │   └── parquet/
        │       └── TABLE.parquet/
        └── schema2/
            └── parquet/
                └── TABLE.parquet/
    """

    root_path = Path(root_path)
    schema_paths = []

    # Case 1:
    # Root itself is a schema
    if validate_folder(root_path):
        schema_paths.append(root_path)

    # Case 2:
    # Child directories may represent additional schemas
    for child in root_path.iterdir():
        if not child.is_dir() or child.name == "parquet":
            continue

        if validate_folder(child):
            schema_paths.append(child)

    return schema_paths


def schema_exists(conn, schema_name):
    query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = '{schema_name}'
        );
    """
    result = execute_query(conn, query)
    return result[0][0] if result else False


def foreign_table_exists(conn, schema_name, table_name):
    """Checks whether a foreign table exists."""
    query = f"""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.foreign_tables
            WHERE foreign_table_schema = '{schema_name}'
              AND foreign_table_name = '{table_name}'
        );
    """

    result = execute_query(conn, query)
    return result[0][0] if result else False
    

def table_creation(
    config, schema_name, table_name, schema_columns, parquet_files, conn
):
    """Creates a foreign table based on the provided schema and Parquet files."""
    try:
        server_name = config["server_name"]

        replace_existing = config.get("table_creation", {}).get(
            "replace_existing", True
        )
        
        table_exists = foreign_table_exists(conn, schema_name, table_name)
        
        if table_exists and not replace_existing:
            message = f"Table '{schema_name}'.'{table_name}' already exists. Skipping."
            log_message(message, info_log_file)
            print(f"\n\t\t{message}\n")

            report["schemas"][schema_name]["tables"][table_name]["status"] = "skipped"
            report["schemas"][schema_name]["tables"][table_name]["skip_reason"] = (
                "Table already exists and replace_existing is false"
            )
            return


        table_query = config["commands"]["foreign_table"].format(
            schema=schema_name,
            table=table_name,
            columns=schema_columns,
            server=server_name,
            file_path=parquet_files,
        )

        if table_exists and replace_existing:
            drop_table_query = config["commands"]["drop_table"].format(
                schema_name=schema_name, table=table_name
            )
            replace_foreign_table(
                conn,
                drop_table_query,
                table_query,
            )
        else:
            execute_query(conn, table_query)

        verify_row_count = config.get("table_creation", {}).get(
            "verify_row_count", True
        )

        if verify_row_count:
            verify_query = config["commands"]["select"].format(
                schema_name=schema_name, table_name=table_name
            )
            result = execute_query(conn, verify_query)
            row_count = result[0][0] if result else 0

            report["schemas"][schema_name]["tables"][table_name]["row_count"] = row_count

            log_message(
                f"Total number of rows for {schema_name}.{table_name}: {row_count}",
                info_log_file,
            )

        message = f"Table '{schema_name}'.'{table_name}' created successfully!"

        log_message(message, info_log_file)
        print(f"\n\t\t{message}\n")

        report["total_tables_processed_successfully"] += 1

    except Exception as e:
        message = f"Error creating table {table_name}: {e}"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        
        report["total_tables_errored_out"] += 1
        report["schemas"][schema_name]["tables"][table_name] = {
            "status": "error",
            "error_message": str(e),
        }


def schema_creation(config, schema_path, conn, spark):
    """Creates a foreign table based on the provided schema and Parquet files."""
    try:
        schema_name = Path(schema_path).name

        schema_already_exists = schema_exists(conn, schema_name)

        schema_query = config["commands"]["create_schema"].format(name=schema_name)
        execute_query(conn, schema_query)

        if schema_already_exists:
            print(f"\n\tSchema '{schema_name}' already exists")
        else:
            print(f"\n\tSchema '{schema_name}' created successfully")

        report["schemas"][schema_name] = {}
        report["schemas"][schema_name]["schema_folder_path"] = str(schema_path)

        app_path = Path(schema_path, "parquet")
        parquet_dirs = [
            item.name 
            for item in Path(app_path).iterdir() 
            if item.is_dir() and item.name.endswith(".parquet")
        ]

        if not parquet_dirs:
            message = f"No Parquet table directories found in schema path: {app_path}"
            log_message(message, error_log_file, level="ERROR")
            print(message)

            report["schemas"][schema_name]["status"] = "error"
            report["schemas"][schema_name]["error_message"] = (
                "No Parquet table directories found"
            )
            return
        
        log_message(f"Parquet directories: {parquet_dirs}", info_log_file)

        for parquet_dir in parquet_dirs:
            table_name = Path(parquet_dir).stem

            if "tables" not in report["schemas"][schema_name]:
                report["schemas"][schema_name]["tables"] = {}
                report["schemas"][schema_name]["tables"][table_name] = {}

            parquet_dir_path = Path(app_path, parquet_dir)
            parquet_files, parquet_files_count  = retrieve_parquet_files(parquet_dir_path)

            if parquet_files is not None:
                schema_columns = get_parquet_schema(
                    spark,
                    str(parquet_dir_path),
                    config,
                    merge_schemas=Path(parquet_dir_path).is_dir(),
                )

                report["schemas"][schema_name]["tables"][table_name] = {
                    "parquet_files_count": parquet_files_count,
                    "parquet_files": parquet_files,
                    "columns_count": len(schema_columns.replace("\n\t", "").split(",")),
                    "column_types": schema_columns.replace("\n\t", "").split(","),
                    "status": "success",
                }

                table_creation(
                    config, schema_name, table_name, schema_columns, parquet_files, conn
                )
            else:
                report["total_tables_errored_out"] += 1
                report["schemas"][schema_name]["tables"][table_name] = {
                    "status": "error",
                    "error_message": f"Parquet files not found in {parquet_dir_path}",
                }

    except Exception as e:
        message = f"Error creating schema {schema_name}: {e}"
        report["schemas"][schema_name]["error_message"] = message
        log_message(message, error_log_file, level="ERROR")
        print(message)

from pathlib import Path


def init_table_creation(conn, config, spark):
    """Creates foreign tables based on the schema extracted from Parquet files."""
    try:
        while True:
            parquet_path = input(
                "\nEnter the Parquet Destination Path containing parquet files "
                "(Path up to the application name folder): "
            ).strip()

            if not parquet_path:
                message = "Error: parquet_path cannot be empty."
                log_message(message, error_log_file, level="ERROR")
                print(message)
                continue

            parquet_path = Path(parquet_path)

            if not parquet_path.is_dir():
                message = (
                    f"{parquet_path} does not exist or is not a directory."
                )
                log_message(message, error_log_file, level="ERROR")
                print(message)
                continue

            break

        report["source_path"] = str(parquet_path)
        report["process"] = "External Table Creation"
        report["schemas"] = {}

        schema_paths = discover_schema_paths(parquet_path)

        if schema_paths:
            print(f"\nDiscovered {len(schema_paths)} schema(s):")

            for schema_path in schema_paths:
                print(f"  - {schema_path.name}")

            for schema_path in schema_paths:
                schema_creation(
                    config,
                    schema_path,
                    conn,
                    spark,
                )
        else:
            message = (
                f"No valid schema directories found under: {parquet_path}"
            )
            log_message(message, error_log_file, level="ERROR")
            print(message)

    except Exception as e:
        message = f"An error occurred while table creation initiation: {e}"
        log_message(message, error_log_file, level="ERROR")
        print(message)
        sys.exit()


def main():
    """Main function to load config, create database connection, and create foreign tables."""
    config = load_config("config.json")
    if not config:
        return

    conn = create_postgres_connection(config)
    if not conn:
        return
    start_time = datetime.now()
    report["start_time"] = start_time.strftime("%Y-%m-%d %H:%M:%S")
    server_name = config["server_name"]

    server_exists = foreign_server_exists(conn, server_name)
    if not server_exists:
        message = f"servername {server_name} does not exists"
        print(message)
        log_message(message, error_log_file, level="ERROR")
        sys.exit()

    report["foreign_server_name"] = server_name
    report["total_tables_processed_successfully"] = 0
    report["total_tables_errored_out"] = 0

    spark = None

    try:
        spark = SparkSession.builder.appName("ExternalTableCreation").getOrCreate()

        init_table_creation(conn, config, spark)

    finally:
        if spark:
            spark.stop()

        close_connection(conn)

    end_time = datetime.now()
    report["end_time"] = end_time.strftime("%Y-%m-%d %H:%M:%S")

    if report.get("schemas"):
        report_path = generate_report(server_name, report)
        log_message(f"Report generated at path: {report_path}", info_log_file)

if __name__ == "__main__":
    main()
