import pandas as pd
import os
import sys
from scripts.identify_headers import identify_headers,identify_number_of_columns
from scripts.detect_delimiter import detect_delimiter_in_sample, detect_record_separator
from scripts.generate_config_file import generate_final_config_file
from scripts.detect_data_type import predict_schema
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.record_seperators import map_record_separator

def read_sample_files(path,has_header, line_number=None):
    data = []
    if os.path.isfile(path):
        # Process a single file
        data.append(process_file(path,has_header, line_number))
    elif os.path.isdir(path):
        # Process all files in the folder
        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            if not os.path.isdir(file_path) and not os.path.basename(file_path) == "config":  # Ignore directories and files in the "config" folder
                data.append(process_file(file_path, has_header, line_number))
    else:
        print(f"Invalid path: {path}")
    
    return pd.DataFrame(data)

def process_file(file_path,has_header, line_number):
    print(f"Reading file: {file_path}")
    with open(file_path, 'r') as file:
        if line_number is not None:
            for _ in range(line_number-1):
                file.readline()  # Skip lines until the specified line
            line = file.readline()
        else:
            line = file.readline()  # Default to the first line if line_number is not specified

        #read Delimiter
        delimiter = detect_delimiter_in_sample(line)
        
        # Read a small sample to detect record separators
        file.seek(0)  # Reset file pointer to the beginning
        sample = file.read(1024)  # Read a sample of the file
        record_separator = map_record_separator(detect_record_separator(sample))

        #Read Number of Columns and Column Names
        num_of_columns=identify_number_of_columns(file_path, delimiter,line_number)
        columns_names=identify_headers(file_path, delimiter, has_header, line_number)
        
        return {'FileName': os.path.abspath(file_path), 'Delimiter': delimiter,
                 'RecordSeparator': record_separator , 'NumOfColumns':num_of_columns, 'ColumnNames':columns_names}
    

if __name__ == "__main__":
    path = r"Y:\Data\Retail\WalmartMX\Development\Pikesh.Maharjan\ETL-AI-Schema-Detection\data\test"
    has_header = True
    header_line_number = 1
    sampling_rate = 10000
    result_df = read_sample_files(path,has_header,header_line_number)
    result_df["HasHeader"] = has_header
    result_df["PredictedSchema"] = None
    for index,rows in result_df.iterrows():
        print(f"File Name: {rows['FileName']}")
        print(f"Delimiter: {rows['Delimiter']}")
        print(f"Record Separator: {rows['RecordSeparator']}")
        print(f"Number of Columns: {rows['NumOfColumns']}")
        print(f"Column Names: {rows['ColumnNames']}")
        schema_df= predict_schema(rows['FileName'],rows['Delimiter'],sampling_rate, has_header) 
        #print(f"Predicted Schema:\n{schema_df}")
        # Store the full result DataFrame (column_name, predicted_data_type, max_length)
        result_df.at[index, "PredictedSchema"] = schema_df
        generate_final_config_file(result_df.loc[index])



