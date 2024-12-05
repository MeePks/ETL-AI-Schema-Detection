import pandas as pd

def process_file(file_path):
    """
    Processes the uploaded file to infer schema and load data.
    
    Args:
        file_path (str): Path to the uploaded file.
    
    Returns:
        dict: Schema details (column names and data types).
    """
    try:
        # Load the file (assume CSV for now)
        df = pd.read_csv(file_path)

        # Infer schema
        schema = {
            "columns": df.columns.tolist(),
            "data_types": df.dtypes.astype(str).to_dict()
        }

        # Example: Log schema to console
        print("Detected Schema:", schema)

        # TODO: Add code to load `df` into a SQL Server database

        return schema
    except Exception as e:
        raise RuntimeError(f"Error processing file: {str(e)}")
