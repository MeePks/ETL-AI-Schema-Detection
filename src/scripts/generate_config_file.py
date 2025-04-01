import pandas as pd

def create_config_files(df,filepath):
    """
    This function accepts a DataFrame, prints its values, and writes each row to a separate Excel file.
    
    Parameters:
    df (pd.DataFrame): The DataFrame containing the configuration data.
    """
    for index, row in df.iterrows():
        # Create a DataFrame for the current row
        row_df = pd.DataFrame([row])
        
        # Print the values of the current row
        print(f"Row {index} values:")
        print(row_df)
        
        # Write the DataFrame to an Excel file
        output_file = f'{filepath}/config_{row["filename"]}.xlsx'
        row_df.to_excel(output_file, index=False)
        
        print(f"Configuration file '{output_file}' has been created.")
