import pandas as pd
import os
import sys
from scripts.identify_headers import identify_headers,identify_number_of_columns
from scripts.detect_delimiter import detect_delimiter_in_sample, detect_record_separator
from scripts.generate_config_file import create_config_files
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
            data.append(process_file(file_path,has_header, line_number))
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
        
        return {'FileName': os.path.basename(file_path), 'delimiter': delimiter,
                 'record_separator': record_separator , 'num_of_columns':num_of_columns, 'columns_names':columns_names}
    

if __name__ == "__main__":
    path = r"Y:\Data\Retail\WalmartMX\Development\Pikesh.Maharjan\ETL-AI-Schema-Detection\Datasets"
    has_header = True
    header_line_number = 1
    result_df = read_sample_files(path,has_header,header_line_number)
    create_config_files(result_df)


