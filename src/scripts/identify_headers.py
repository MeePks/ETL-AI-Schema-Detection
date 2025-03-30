import os

# Function to identify the number of columns in a file
def identify_number_of_columns(file_path, delimiter, line_number=None):
    with open(file_path, 'r') as file:
        if line_number is not None:
            for _ in range(line_number-1):
                file.readline()  # Skip lines until the specified line
            line = file.readline().strip()
        else:
            line = file.readline().strip()  # Default to the first line if line_number is not specified
        
        columns = line.split(delimiter)
        return len(columns)

# Function to identify headers in a file
def identify_headers(file_path, delimiter, has_header=True, header_line_number=None):
    with open(file_path, 'r') as file:
        if has_header:
            if header_line_number is not None:
                for _ in range(header_line_number):
                    file.readline()  # Skip lines until the specified line
                header_line = file.readline().strip()
            else:
                header_line = file.readline().strip()  # Default to the first line if header_line_number is not specified
            
            headers = header_line.split(delimiter)
        else:
            num_columns = identify_number_of_columns(file_path, delimiter)
            headers = [f'field{i+1}' for i in range(num_columns)]
        
        return headers

