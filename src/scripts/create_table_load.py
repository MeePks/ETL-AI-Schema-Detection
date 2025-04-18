import json
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, Date, MetaData, inspect, Numeric, Text , Identity , BigInteger
from urllib.parse import quote_plus
import pandas as pd
from datetime import datetime
import logging
import os


def create_connection(server, database):
    try:
        params = quote_plus(
            f"DRIVER=ODBC Driver 17 for SQL Server;"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes;"
        )
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")
        return engine
    except Exception as e:
        print(f"Error creating connection: {e}")

def is_valid_date(value, formats=["%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y", "%m-%d-%Y"]):
    for fmt in formats:
        try:
            datetime.strptime(value, fmt)
            return True
        except ValueError:
            continue
    return False

def create_sql_table(config_data, engine,mode="append"):
    metadata = MetaData()
    table_name = config_data["file_name"].split("\\")[-1].split(".")[0]  # Derive a table name from the file name

    columns = []
    for column in config_data["columns"]:
        column_name = column["name"]
        data_type = column["type"]
        length = column.get("length", None)

        if data_type == "varchar" and length:
            columns.append(Column(column_name, String(length)))
        elif data_type == "float":
            precision = column.get("length", 18)  # total digits
            scale = column.get("scale", 4)        # digits after decimal
            columns.append(Column(column_name, Numeric(precision, scale)))
        elif data_type == "int":
            columns.append(Column(column_name, Integer))
        elif data_type == "bigint":
            columns.append(Column(column_name, BigInteger))
        elif data_type == "date":
            columns.append(Column(column_name, Date))
    
    columns.append(Column("FileName", String(255), nullable=False))
    columns.append(Column("AutoID", Integer, Identity(start=1), primary_key=True))

    table = Table(table_name, metadata, *columns)

    # Use inspector to check if the table exists
    inspector = inspect(engine)
    if inspector.has_table(table_name) and mode == "append":
        print(f" Table '{table_name}' already exists. Skipping creation.")
    elif inspector.has_table(table_name) and mode == "replace":
        with engine.connect() as connection:
            metadata.tables[table_name].drop(connection)
            metadata.create_all(connection)
            connection.commit()
        print(f"Table {table_name} replaced successfully.")
    else:
        with engine.connect() as connection:
            metadata.create_all(connection)
            connection.commit()
        print(f"Table {table_name} created successfully.")
    
    return table 

def find_column_issue_sqlalchemy(table, row):
    SQL_SERVER_INT_MAX = 2_147_483_647
    SQL_SERVER_INT_MIN = -2_147_483_648
    error_msg = None
    for column in table.columns:
        col_name = column.name
        col_type = column.type

        if col_name not in row:
            continue

        value = row.get(col_name)

        try:
            if isinstance(col_type, Integer) and not isinstance(col_type, BigInteger):
                val = int(value)
                if val < SQL_SERVER_INT_MIN or val > SQL_SERVER_INT_MAX:
                    error_msg = f"Column '{col_name}' with value '{value}' is out of range for SQL Server INT type."
            elif  isinstance(col_type, BigInteger):
                val = int(value)
            elif isinstance(col_type, Float):
                float(value)
            elif isinstance(col_type, Numeric):
                # Handle precision/scale overflow
                float(value)
            elif isinstance(col_type, Date):
                if isinstance(value, str):
                    parsed = False
                    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
                        try:
                            datetime.strptime(value, fmt)
                            parsed = True
                            break
                        except ValueError:
                            continue
                    if not parsed:
                        error_msg = f"Column '{col_name}' with value '{value}' is not a valid date."
                        print(f"Column '{col_name}' with value '{value}' is not a valid date.")
            elif isinstance(col_type, String):
                str_val=str(value)
                if col_type.length is not None and len(str_val) > col_type.length:
                    error_msg = f"Column '{col_name}' with value '{value}' exceeds max length {col_type.length}."
                    print(f"Column '{col_name}' with value '{value}' exceeds max length {col_type.length}.")
            else:
                print(f"Column '{col_name}': Unknown type {type(col_type)}. Skipping check.")
        except Exception as e:
            print(f"Column '{col_name}' with value '{value}' caused error: {e}")
            error_msg = f"Column '{col_name}' with value '{value}' caused error: {e}"
    
    return error_msg

def setup_logger(data_file_path):
    base_dir=os.path.dirname(data_file_path)
    base_name = os.path.basename(data_file_path)
    file_name_wo_ext = os.path.splitext(base_name)[0]
    log_file_path = os.path.join(base_dir, f"{file_name_wo_ext}_load_logs.log")

    logger = logging.getLogger(file_name_wo_ext)
    logger.setLevel(logging.INFO)

    # Prevent duplicate handlers
    if not logger.handlers:
        fh = logging.FileHandler(log_file_path, mode='w')
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    logger.info("Log started for data load")
    return logger

def load_data_to_sql(file_path,config_data, table_name, engine, delimiter=",", has_header=True, start_row=0,insert_chunksize=1000,mode="append"):
    table_schema=create_sql_table(config_data, engine,mode=mode)
    header_option = 0 if has_header else None
    skiprows = start_row if has_header else range(start_row)

    #  --- setup Logging ----
    logger = setup_logger(file_path)
    
    df = pd.read_csv(file_path, delimiter=delimiter, header=header_option, skiprows=skiprows)
    df["FileName"] = file_path.split("\\")[-1]

    # Reflect the table from the database to match column mappings
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables.get(table_name)

    if table is None:
        print(f"Table {table_name} not found in the database.")
        logger.error(f"Table {table_name} not found in the database.")
        return
    
    total_records_read = 0
    total_records_inserted = 0
    error_rows = []

    # Insert data into SQL Server
    start_time = datetime.now()
    logger.info(f"Loading data from \nFilePath: {file_path} \nTable: {table_name}")
    with engine.begin() as connection:
        for start in range(0, len(df), insert_chunksize):
            chunk = df.iloc[start:start+insert_chunksize]
            total_records_read += len(chunk)
            try:
                chunk.to_sql(
                    table_name,
                    con=connection,
                    if_exists='append',
                    index=False,
                    chunksize=insert_chunksize,
                    method='multi'
                )
                total_records_inserted += len(chunk)
                print(f"Successfully inserted rows {start+1} to {start+ len(chunk) }")
            except Exception as e:
                for i, row in chunk.iterrows():
                    try:
                        row_df = pd.DataFrame([row])
                        row_df.to_sql(
                            table_name,
                            con=connection,
                            if_exists='append',
                            index=False,
                            method='multi'
                        )
                        total_records_inserted += 1
                    except Exception as row_error:
                        print(f"Error at row {i+1}:row:{row.to_dict()} ")
                        logger.error(f"Error at row {i+1} : \n row:{row.to_dict()}")
                        error_msg=find_column_issue_sqlalchemy(table_schema, row)
                        logger.error(f"{error_msg}")
                        end_time = datetime.now()
                        duration = end_time - start_time
                        logger.info(f"Data load completed with Errors in {duration}")
                        logger.info(f"Total records read: {total_records_read}")
                        logger.info(f"Total records inserted: {total_records_inserted}")
                        return
                


    end_time = datetime.now()
    duration = end_time - start_time
    logger.info(f"Data load successfully completed in {duration}")
    logger.info(f"Total records read: {total_records_read}")
    logger.info(f"Total records inserted: {total_records_inserted}")
    print(f"Data loaded successfully into {table_name}")



'''
if __name__ == "__main__":
    config_data = r"Y:\Data\Retail\WalmartMX\Development\Pikesh.Maharjan\ETL-AI-Schema-Detection\config\file_1.csv_config.json"
    with open(config_data, 'r') as file:
        config_data = json.load(file)
    server = "DC-Carbon"
    database = "AmazonDataLoad"
    table_name = config_data["file_name"].split("\\")[-1].split(".")[0]
    engine = create_connection(server, database)
    file_path=r"Y:\Data\Retail\WalmartMX\Development\Pikesh.Maharjan\ETL-AI-Schema-Detection\data\samples\file_1.csv"
    load_data_to_sql(
        file_path, 
        config_data,
        table_name, 
        engine, 
        delimiter=config_data["delimiter"],
        has_header=config_data["has_header"], 
        start_row=0,
        insert_chunksize=100,
        mode="replace")
'''