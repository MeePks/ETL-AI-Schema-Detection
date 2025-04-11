import json
from sqlalchemy import create_engine, Table, Column, Integer, Float, String, Date, MetaData, inspect, Numeric, Text , Identity
from urllib.parse import quote_plus

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

def create_sql_table(config_data, engine):
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
        elif data_type == "date":
            columns.append(Column(column_name, Date))
    
    columns.append(Column("FileName", String(255), nullable=False))
    columns.append(Column("AutoID", Integer, Identity(start=1), primary_key=True))

    table = Table(table_name, metadata, *columns)

    # Use inspector to check if the table exists
    inspector = inspect(engine)
    if inspector.has_table(table_name):
        print(f" Table '{table_name}' already exists. Skipping creation.")
    else:
        with engine.connect() as connection:
            metadata.create_all(connection)
            connection.commit()
        print(f"Table {table_name} created successfully.")

import pandas as pd
from sqlalchemy import insert, Table, MetaData

def load_data_to_sql(file_path,config_data, table_name, engine, delimiter=",", has_header=True, start_row=0):
    create_sql_table(config_data, engine)
    header_option = 0 if has_header else None
    skiprows = start_row if has_header else range(start_row)
    
    df = pd.read_csv(file_path, delimiter=delimiter, header=header_option, skiprows=skiprows)
    df["FileName"] = file_path.split("\\")[-1]
    print(df.head())

    print(df.columns)

    # Reflect the table from the database to match column mappings
    metadata = MetaData()
    metadata.reflect(bind=engine)
    table = metadata.tables.get(table_name)

    if table is None:
        print(f"Table {table_name} not found in the database.")
        return

    # Insert data into SQL Server
    with engine.begin() as connection:
        #connection.execute(table.insert(), df.to_dict(orient="records"))
        df.to_sql(table_name, con=connection, if_exists='append', index=False, chunksize=1, method='multi',index_label=False)
        connection.commit()

    print(f"Data loaded successfully into {table_name}")

        
if __name__ == "__main__":
    config_data = r"Y:\Data\Retail\WalmartMX\Development\Pikesh.Maharjan\ETL-AI-Schema-Detection\config\file_3.txt_config.json"
    with open(config_data, 'r') as file:
        config_data = json.load(file)
    server = "DC-Carbon"
    database = "AmazonDataLoad"
    table_name = config_data["file_name"].split("\\")[-1].split(".")[0]
    engine = create_connection(server, database)
    file_path=r"Y:\Data\Retail\WalmartMX\Development\Pikesh.Maharjan\ETL-AI-Schema-Detection\data\raw\file_3.txt"
    load_data_to_sql(file_path, config_data,table_name, engine, delimiter=config_data["delimiter"], has_header=config_data["has_header"], start_row=0)
