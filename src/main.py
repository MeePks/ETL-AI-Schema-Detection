import os
import sys
import pandas as pd

# Add the scripts directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'scripts')))

# Import functions from your scripts
from detect_delimiters import read_sample_files, detect_delimiter_in_sample
from identify_headers import identify_number_of_columns, identify_headers

# Example usage
def main():
    # Path to your file or folder
    path = r'path_to_your_file_or_folder'
    line_number = 1  # Specify the line number to use for delimiter detection

    # Step 1: Detect delimiters and record separators
    df = read_sample_files(path, line_number)
    print("Delimiter and Record Separator Detection:")
    print(df.to_string(index=False))

    # Assuming the first file in the DataFrame for further processing
    if not df.empty:
        file_info = df.iloc[0]
        file_path = os.path.join(path, file_info['FileName'])
        delimiter = file_info['delimiter']

        # Step 2: Identify number of columns
        num_columns = identify_number_of_columns(file_path, delimiter, line_number)
        print(f"Number of columns: {num_columns}")

        # Step 3: Identify headers
        headers = identify_headers(file_path, delimiter, has_header=True, header_line_number=line_number)
        print(f"Identified headers: {headers}")

if __name__ == "__main__":
    main()
