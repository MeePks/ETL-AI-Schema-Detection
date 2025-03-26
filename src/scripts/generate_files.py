import random
import string
from datetime import datetime
import os

def generate_random_data():
    data = []
    data.append(str(random.randint(1, 100)))  # int
    data.append(str(random.uniform(1, 100)))  # float
    data.append(str(random.randint(1000000000, 9999999999)))  # bigint
    data.append(datetime.now().strftime(random.choice(['%Y-%m-%d', '%m/%d/%Y', '%d-%m-%Y'])))  # date with different formats
    data.append(''.join(random.choices(string.ascii_letters, k=10)))  # varchar
    return data

def generate_files():
    output_dir = 'data\samples' 

    delimiters = [',', '|', '\t', ';', ':']
    record_separators = ['\n', '\r\n'] 

    for i in range(500):  # Generate 500 files
        delimiter = random.choice(delimiters)  # Randomly choose a delimiter
        record_separator = random.choice(record_separators)
        if delimiter == ',':
            delimiter_name = 'Comma'
        elif delimiter == '|':
            delimiter_name = 'Pipe'
        elif delimiter == '\t':
            delimiter_name = 'Tab'
        elif delimiter == ';':
            delimiter_name = 'Semicolon'
        else:
            delimiter_name = 'Colon'
        file_name = f'{delimiter_name}_DeLimited_{i}.txt'
        file_path = os.path.join(output_dir, file_name)

        with open(file_path, 'w') as file:
            for _ in range(10000):  # Each file will contain 10 rows
                data = generate_random_data()
                line = delimiter.join(data)
                file.write(line + record_separator)

        print(f'Generated file: {file_path}')

generate_files()

