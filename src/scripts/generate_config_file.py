import json
import os
import pandas as pd



def create_excel_file(config_data,output_path):
    file_metadata = {
    "File Name": config_data["file_name"],
    "Delimiter": config_data["delimiter"],
    "Record Separator": config_data["record_separator"],
    "Has Header": config_data["has_header"]
}
    file_metadata_df = pd.DataFrame(list(file_metadata.items()), columns=["Attribute", "Value"])

    # Convert columns metadata to DataFrame
    columns_data = config_data["columns"]
    columns_df = pd.DataFrame(columns_data)

    # Write to Excel with multiple sheets
    output_path = os.path.join(os.path.abspath(output_path).replace('.json',''),os.path.basename(config_data["file_name"]) + "_config.xlsx")
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Write file metadata to the first sheet
        file_metadata_df.to_excel(writer, sheet_name="File Metadata", index=False)

        # Write columns data to the second sheet
        columns_df.to_excel(writer, sheet_name="Columns", index=False)

        # Optional: Add a table style to the columns sheet for better visualization
        worksheet = writer.sheets["Columns"]
        worksheet.auto_filter.ref = worksheet.dimensions  # Add auto-filter for columns
        worksheet.table = {
            'range': worksheet.dimensions,
            'name': 'ColumnsTable',
            'columns': [{'header': 'Name'}, {'header': 'Type'}, {'header': 'Length'}],
        }

def generate_final_config_file(file_metadata_df, output_dir="config"):
    os.makedirs(output_dir, exist_ok=True)
    config_data = {
        "file_name": file_metadata_df["FileName"],
        "delimiter": file_metadata_df["Delimiter"],
        "record_separator": file_metadata_df["RecordSeparator"],
        "has_header": bool(file_metadata_df["HasHeader"]),
        "columns": []
    }

    for col, dtype, maxlen in zip(file_metadata_df["PredictedSchema"]["column_name"],
                                    file_metadata_df["PredictedSchema"]["predicted_data_type"],
                                    file_metadata_df["PredictedSchema"]["max_length"]):
        column_config = {
            "name": col,
            "type": dtype,
        }
        if dtype == "varchar":
            column_config["length"] = int(maxlen)
        elif dtype == "float":
            column_config["length"] = int(maxlen)
            column_config["scale"] = 2
        config_data["columns"].append(column_config)

    output_path = os.path.join(output_dir, f"{os.path.basename(file_metadata_df['FileName'])}_config.json")
    with open(output_path, "w") as f:
        json.dump(config_data, f, indent=4)
    create_excel_file(config_data,output_path)
    print(f"Config saved: {output_path}")
    return output_path

