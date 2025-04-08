import os
import string
import pandas as pd
import csv
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config.record_seperators import map_record_separator
import os

# Function to read sample files and extract delimiters and record separators
def read_sample_files(path, line_number=None):
    data = []
    if os.path.isfile(path):
        # Process a single file
        data.append(process_file(path, line_number))
    elif os.path.isdir(path):
        # Process all files in the folder
        for file_name in os.listdir(path):
            file_path = os.path.join(path, file_name)
            data.append(process_file(file_path, line_number))
    else:
        print(f"Invalid path: {path}")
    
    return pd.DataFrame(data)

# Function to process a single file
def process_file(file_path, line_number):
    print(f"Reading file: {file_path}")
    with open(file_path, 'r') as file:
        if line_number is not None:
            for _ in range(line_number-1):
                file.readline()  # Skip lines until the specified line
            line = file.readline()
        else:
            line = file.readline()  # Default to the first line if line_number is not specified
        delimiter = detect_delimiter_in_sample(line)
        
        # Read a small sample to detect record separators
        file.seek(0)  # Reset file pointer to the beginning
        sample = file.read(1024)  # Read a sample of the file
        record_separator = map_record_separator(detect_record_separator(sample))
        
        return {'FileName': os.path.basename(file_path), 'delimiter': delimiter, 'record_separator': record_separator}

# Function to detect delimiter in sample text
def detect_delimiter_in_sample(text):
    # Consider punctuation, whitespace, and common record separators as potential delimiters
    potential_delimiters = string.punctuation.replace('"', '').replace('_','') + string.whitespace
    
    # Count occurrences of each character
    delimiter_counts = {char: text.count(char) for char in potential_delimiters}
    
    # Find the character with the highest count
    most_frequent_delimiter = max(delimiter_counts, key=delimiter_counts.get)
    #print(f"Detected delimiter: {most_frequent_delimiter}")
    
    # Return the most frequent character if its count is significant
    return most_frequent_delimiter if delimiter_counts[most_frequent_delimiter] > 1 else None

# Function to detect record separator in sample text
def detect_record_separator(sample):
        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample)
            #print(f"Detected record separator: {map_record_separator(dialect.lineterminator)}")
            return dialect.lineterminator
        except:
            return '\n'
        
'''
# Example usage
path = r'Y:\Split\Retail\GiantEagle\ScanBack\2025-03-18\gescanback_20250318.dat.txt'
line_number = 1  # Specify the line number to use for delimiter detection 
df = read_sample_files(path, line_number)
print(df.to_string(index=False))
'''