import os
import csv
import pandas as pd

input_dir = rf"data\samples"  # adjust path if needed
output_file = rf"features\parsed_data.csv"
parsed_rows = []

def extract_label(col_name):
    # Assumes col_name format: "columnname_datatype"
    return col_name.split("_")[-1]

for filename in os.listdir(input_dir):
    if filename.endswith((".csv", ".txt", ".dat")):
        file_path = os.path.join(input_dir, filename)
        with open(file_path, "r", encoding="utf-8") as f:
            try:
                dialect = csv.Sniffer().sniff(f.read(1024))
                f.seek(0)
                reader = csv.reader(f, delimiter=dialect.delimiter)
                headers = next(reader)
                labels = [extract_label(col) for col in headers]
                
                for row in reader:
                    for i, val in enumerate(row):
                        parsed_rows.append({
                            "value": val,
                            "label": labels[i],
                            "source_file": filename,
                            "column_index": i
                        })
            except Exception as e:
                print(f"Skipping {filename} due to error: {e}")

df = pd.DataFrame(parsed_rows)
os.makedirs(os.path.dirname(output_file), exist_ok=True)
df.to_csv(output_file, index=False)
print(f"✅ Parsed {len(df)} rows into {output_file}")
