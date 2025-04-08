import os
import csv
import random
from faker import Faker
from datetime import datetime, timedelta

# Initialize
fake = Faker()
output_dir = rf"data\samples"
os.makedirs(output_dir, exist_ok=True)

# Configurations
delimiters = [",", "|", ";"]
file_extensions = ["csv", "txt", "dat"]
date_formats = ["%Y-%m-%d", "%d/%m/%Y", "%m-%d-%Y", "%b %d, %Y"]

# Column data generators
def get_value(col_type, date_fmt):
    if col_type == "int":
        return str(random.randint(1, 10000))
    elif col_type == "float":
        return f"{random.uniform(1, 1000):.2f}"
    elif col_type == "varchar":
        return fake.word()
    elif col_type == "date":
        return fake.date_between(start_date='-5y', end_date='today').strftime(date_fmt)

# Create schema for each file
def generate_schema():
    col_count = random.randint(3, 8)
    schema = []
    for _ in range(col_count):
        col_type = random.choice(["int", "float", "varchar", "date"])
        col_name = f"{fake.word()}_{col_type}"
        schema.append((col_name, col_type))
    return schema


# Function to get delimiter based on file extension
def get_delimiter(extension):
    if extension == "csv":
        return ","  # Always comma for CSV
    else:
        return random.choice(["\t", "|", ";", ",", "^"])  # Random for others

# Main generation loop
for i in range(500):
    ext = random.choice(file_extensions)
    delimiter=get_delimiter(ext)
    row_count = random.randint(1000, 10000)
    date_format = random.choice(date_formats)

    schema = generate_schema()
    file_path = os.path.join(output_dir, f"file_{i}.{ext}")

    with open(file_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=delimiter)

        
        writer.writerow([col[0] for col in schema])

        for _ in range(row_count):
            row = [get_value(col_type, date_format) for _, col_type in schema]
            writer.writerow(row)

print("✅ 500 diverse synthetic files generated in './generated_files'.")
