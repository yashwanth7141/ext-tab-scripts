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


def retrieve_merge_parquet_files(merge_paths):
    """Retrieve all actual Parquet files from the supplied merge paths."""
    all_parquet_files = []

    for parquet_path in merge_paths:
        try:
            parquet_path = Path(parquet_path)

            parquet_files = [
                str(file)
                for file in parquet_path.rglob("*.parquet")
                if file.is_file() and not file.name.startswith(".")
            ]

            if not parquet_files:
                message = (
                    f"No Parquet files found in merge path: {parquet_path}"
                )
                log_message(
                    message,
                    error_log_file,
                    level="ERROR",
                )
                print(message)
                return None

            all_parquet_files.extend(parquet_files)

        except Exception as e:
            message = (
                f"Error retrieving Parquet files from "
                f"{parquet_path}: {e}"
            )
            log_message(
                message,
                error_log_file,
                level="ERROR",
            )
            print(message)
            return None

    return all_parquet_files


def create_merged_table(
    config,
    target_schema,
    table_name,
    schema_columns,
    parquet_files,
    conn,
    report,
    merge_paths
):
    """
    Create a foreign table using Parquet files from multiple applications.
    """

    try:
        table_creation_config = config.get("table_creation", {})

        replace_existing = table_creation_config.get(
            "replace_existing", False
        )
        verify_row_count = table_creation_config.get(
            "verify_row_count", False
        )

        # ---------------------------------------------------------
        # 1. Create target schema
        # ---------------------------------------------------------
        schema_already_exists = schema_exists(conn, target_schema)

        schema_query = config["commands"]["create_schema"].format(
            name=target_schema
        )

        execute_query(conn, schema_query)

        if schema_already_exists:
            print(f"\nSchema '{target_schema}' already exists.")
        else:
            print(f"\nSchema '{target_schema}' created successfully.")

        table_exists = foreign_table_exists(
            conn,
            target_schema,
            table_name,
        )

        if table_exists and not replace_existing:

            message = (
                f"Table '{target_schema}'.'{table_name}' already exists. "
                "Skipping."
            )

            log_message(message, info_log_file)

            print(f"\n{message}\n")

            report.setdefault("schemas", {})

            report["schemas"].setdefault(
                target_schema,
                {
                    "schema_folder_path": "Merged Applications",
                    "tables": {},
                },
            )

            report["schemas"][target_schema]["tables"][table_name] = {
                "status": "skipped",
                "merge": True,
                "skip_reason": (
                    "Table already exists and replace_existing is false"
                ),
                "source_applications": [
                    path.parent.parent.name
                    for path in merge_paths
                ],
                "source_paths": [
                    str(path)
                    for path in merge_paths
                ],
            }

            return "skipped"

        # ---------------------------------------------------------
        # 2. Drop existing table if replacement is enabled
        # ---------------------------------------------------------
        if table_exists and replace_existing:

            drop_query = config["commands"]["drop_table"].format(
                schema_name=target_schema,
                table=table_name,
            )

            log_message(
                f"Replacing existing merged table "
                f"'{target_schema}.{table_name}'.",
                info_log_file,
            )

            execute_query(conn, drop_query)

        # ---------------------------------------------------------
        # 3. Prepare Parquet file paths
        # ---------------------------------------------------------
        file_path = " ".join(parquet_files)

        # ---------------------------------------------------------
        # 4. Create foreign table
        # ---------------------------------------------------------
        table_query = config["commands"]["foreign_table"].format(
            schema=target_schema,
            table=table_name,
            columns=schema_columns,
            server=config["server_name"],
            file_path=file_path,
        )

        log_message(
            f"Creating merged foreign table "
            f"'{target_schema}.{table_name}'...",
            info_log_file,
        )

        execute_query(conn, table_query)

        print(
            f"Foreign table '{target_schema}.{table_name}' "
            f"created successfully."
        )

        # ---------------------------------------------------------
        # 5. Optional row-count verification
        # ---------------------------------------------------------
        row_count = None

        if verify_row_count:
            select_query = config["commands"]["select"].format(
                schema_name=target_schema,
                table_name=table_name,
            )

            result = execute_query(conn, select_query)

            if result:
                row_count = result[0][0]

            print(
                f"Row count for '{target_schema}.{table_name}': "
                f"{row_count}"
            )

        # ---------------------------------------------------------
        # 6. Update report
        # ---------------------------------------------------------
        report.setdefault("schemas", {})

        report["schemas"].setdefault(
            target_schema,
            {
                "schema_folder_path": "Merged Applications",
                "tables": {},
            },
        )

        column_types = (
            schema_columns
            .replace("\n\t", "")
            .split(",")
        )

        table_report = {
            "parquet_files_count": len(parquet_files),
            "parquet_files": parquet_files,
            "columns_count": len(column_types),
            "column_types": column_types,
            "status": "success",
            "merge": True,
            "source_applications": [
                path.parent.parent.name
                for path in merge_paths
            ],
            "source_paths": [
                str(path)
                for path in merge_paths
            ],
        }

        if verify_row_count:
            table_report["row_count"] = row_count

        report["schemas"][target_schema]["tables"][table_name] = table_report

        report["total_tables_processed_successfully"] += 1

        return "created"

    except Exception as e:
        message = (
            f"Error creating merged table "
            f"'{target_schema}.{table_name}': {e}"
        )

        log_message(
            message,
            error_log_file,
            level="ERROR",
        )

        print(message)

        return "failed"


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

        if isinstance(parquet_path, (list, tuple)):
            df = read_option.parquet(*parquet_path)
        else:
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

        parquet_dir = child / "parquet"

        if not parquet_dir.is_dir():
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



def init_table_creation(conn, config, spark):
    """Creates foreign tables based on the schema extracted from Parquet files."""
    try:
        while True:
            creation_flag = (
                input(
                    "\nHow would you like to create External Tables?\n"
                    "\n1. Create External Tables for a Single Application Name (Schema)"
                    "\n2. Create External Tables by Merging Multiple Application Names (Schemas)"
                    "\nOption (1/2) [default 1]: "
                ).strip()
                or "1"
            )

            if creation_flag in {"1", "2"}:
                break

            print("Invalid option. Please enter '1' or '2'.")

        report["process"] = "External Table Creation"
        report["schemas"] = {}

        # ---------------------------------------------------------
        # Option 1: Single Application
        # ---------------------------------------------------------
        if creation_flag == "1":

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

        # ---------------------------------------------------------
        # Option 2: Merge Multiple Applications
        # ---------------------------------------------------------
        else:
            report["process"] = "External Table Creation - Merge Applications"

            merge_paths = []

            print(
                "\nEnter the Parquet paths to merge."
                "\nEnter one path per application name (schema)."
                "\nType 'done' when all paths have been entered."
            )

            path_number = 1

            while True:
                parquet_path = input(
                    f"\nPath {path_number}: "
                ).strip()

                if parquet_path.lower() == "done":
                    if not merge_paths:
                        print("At least one Parquet path is required.")
                        continue

                    break

                if not parquet_path:
                    message = "Error: parquet path cannot be empty."
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

                if not parquet_path.name.endswith(".parquet"):
                    message = (
                        f"Invalid Parquet path: {parquet_path}\n"
                        "Please enter the path up to the "
                        "<table_name>.parquet directory."
                    )
                    log_message(message, error_log_file, level="ERROR")
                    print(message)
                    continue

                if parquet_path in merge_paths:
                    message = f"Path already entered: {parquet_path}"
                    print(message)
                    continue

                merge_paths.append(parquet_path)
                path_number += 1

            print(f"\nDiscovered {len(merge_paths)} application path(s):")

            for path in merge_paths:
                print(f"  - {path}")

            # -----------------------------------------------------
            # Validate logical table names
            # -----------------------------------------------------

            table_names = {
                path.name.removesuffix(".parquet")
                for path in merge_paths
            }

            if len(table_names) != 1:
                message = (
                    "The Parquet paths belong to different table names. "
                    "All paths must belong to the same logical table.\n"
                    f"Discovered tables: {', '.join(sorted(table_names))}"
                )
                log_message(message, error_log_file, level="ERROR")
                print(message)
                return

            table_name = table_names.pop()

            # -----------------------------------------------------
            # Get target schema
            # -----------------------------------------------------

            while True:
                target_schema = input(
                    "\nEnter target schema name (Application Name)for the merged External Table: "
                ).strip()

                if not target_schema:
                    print("Target schema name cannot be empty.")
                    continue

                break

            # -----------------------------------------------------
            # Display merge group
            # -----------------------------------------------------

            print("\n" + "-" * 50)
            print("Merge Group")
            print("-" * 50)
            print(f"Target Schema : {target_schema}")
            print(f"Table         : {table_name}")
            print("\nSource Applications:")

            for path in merge_paths:
                application_name = path.parent.parent.name
                print(f" - {application_name}")

            print("-" * 50)

            # -----------------------------------------------------
            # Confirmation
            # -----------------------------------------------------

            while True:
                confirmation = input(
                    "\nCreate merged External Table? (Y/N): "
                ).strip().lower()

                if confirmation in {"y", "n"}:
                    break

                print("Invalid option. Please enter 'Y' or 'N'.")

            if confirmation == "n":
                print("Merged External Table creation cancelled.")
                return

            # -----------------------------------------------------
            # Store merge information for next processing step
            # -----------------------------------------------------

            report["source_path"] = [str(path) for path in merge_paths]

            print("\nMerge configuration accepted.")

            # -----------------------------------------------------
            # Retrieve actual Parquet files
            # -----------------------------------------------------

            parquet_files = retrieve_merge_parquet_files(merge_paths)

            if not parquet_files:
                message = "No Parquet files found for the merge group."
                log_message(
                    message,
                    error_log_file,
                    level="ERROR",
                )
                print(message)
                return

            print(
                f"\nDiscovered {len(parquet_files)} Parquet file(s) "
                f"for merging:"
            )

            for parquet_file in parquet_files:
                print(f" - {parquet_file}")

            # -----------------------------------------------------
            # Extract merged schema
            # -----------------------------------------------------

            schema_columns = get_parquet_schema(
                spark=spark,
                parquet_path=parquet_files,
                config=config,
                merge_schemas=True,
            )

            if not schema_columns:
                message = (
                    f"Unable to extract schema for merged table "
                    f"{table_name}."
                )
                log_message(
                    message,
                    error_log_file,
                    level="ERROR",
                )
                print(message)
                return

            print(
                f"\nSchema successfully extracted for "
                f"merged table '{table_name}':"
            )
            print(schema_columns)

            result = create_merged_table(
                config=config,
                target_schema=target_schema,
                table_name=table_name,
                schema_columns=schema_columns,
                parquet_files=parquet_files,
                conn=conn,
                report=report,
                merge_paths=merge_paths
            )

            if result == "created":
                print(
                    f"\nMerged External Table '{target_schema}.{table_name}' "
                    f"created successfully."
                )

            elif result == "skipped":
                print(
                    f"\nMerged External Table '{target_schema}.{table_name}' "
                    f"skipped because it already exists."
                )

            else:
                print(
                    f"\nFailed to create merged table "
                    f"'{target_schema}.{table_name}'."
                )

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
        spark = (
            SparkSession.builder
            .appName("ExternalTableCreation")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

        spark.sparkContext.setLogLevel("ERROR")

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
