import pandas as pd
import os

def create_config_files(df, filepath):
    """
    This function accepts a DataFrame, prints its values, and writes each row to a separate Excel file.

    Parameters:
    df (pd.DataFrame): The DataFrame containing the configuration data.
    filepath (str): The base file path where the configuration files will be created.
    """
    for index, row in df.iterrows():
        # Create a DataFrame for the current row
        row_df = df.iloc[[index]].copy()
        output_file = f'{filepath}/config/config_{os.path.splitext(row["FileName"])[0]}.xlsx'
        output_directory = os.path.dirname(output_file)

        # Create the directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)
        print(f"Creating configuration file '{output_file}'...")

        # Write the row DataFrame to Excel
        row_df.to_excel(output_file, index=False)
        print(f"Configuration file '{output_file}' has been created.")
