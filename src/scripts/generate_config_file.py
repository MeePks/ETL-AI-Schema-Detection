import json
import os

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
        config_data["columns"].append(column_config)

    output_path = os.path.join(output_dir, f"{os.path.basename(file_metadata_df['FileName'])}_config.json")
    with open(output_path, "w") as f:
        json.dump(config_data, f, indent=4)
    print(f"Config saved: {output_path}")
