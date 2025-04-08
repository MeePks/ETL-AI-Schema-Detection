import pandas as pd
import re
from concurrent.futures import ProcessPoolExecutor
import time

# Read the CSV data
df = pd.read_csv("features/parsed_data.csv")

# Define the feature extraction function
def extract_features(val):
    features = {}
    str_val = str(val)

    features["length"] = len(str_val)
    features["num_digits"] = sum(c.isdigit() for c in str_val)
    features["num_letters"] = sum(c.isalpha() for c in str_val)
    features["num_special_chars"] = sum(not c.isalnum() for c in str_val)

    features["contains_alpha"] = any(c.isalpha() for c in str_val)
    features["contains_digit"] = any(c.isdigit() for c in str_val)
    features["contains_date_sep"] = int(bool(re.search(r"[/-]", str_val)))

    # Type-casting checks
    try:
        float_val = float(str_val)
        features["is_float"] = 1
        features["is_int"] = int(float_val.is_integer())
    except:
        features["is_float"] = 0
        features["is_int"] = 0

    features["num_tokens"] = len(str_val.split())
    features["is_uppercase"] = str_val.isupper()

    return pd.Series(features)

# Define the function to process data in batches with parallelism
def process_batch(batch_start, batch_size):
    batch_df = df.iloc[batch_start:batch_start + batch_size]  # Slice batch
    print(f"Processing batch: {batch_start} to {batch_start + batch_size - 1}")
    batch_feature_df = batch_df["value"].apply(extract_features)
    batch_feature_df["label"] = batch_df["label"]  # Include label for supervised training
    return batch_feature_df

# Parameters
batch_size = 100000  # Adjust this value as needed based on system memory
total_rows = len(df)
batches = (total_rows // batch_size) + (1 if total_rows % batch_size else 0)

# Start timer for the process
start_time = time.time()

# Using ProcessPoolExecutor for parallel processing
def main():
    engineered_features = pd.DataFrame()  # Declare inside the main function
    
    with ProcessPoolExecutor() as executor:
        futures = []
        
        # Create futures for all batches
        for batch_start in range(0, total_rows, batch_size):
            futures.append(executor.submit(process_batch, batch_start, batch_size))
        
        # Wait for all futures to complete and gather the results
        for future in futures:
            result = future.result()
            engineered_features = pd.concat([engineered_features, result], ignore_index=True)

    # Save the results to CSV
    engineered_features.to_csv("features/engineered_features.csv", index=False)

    # Print the total processing time
    end_time = time.time()
    print(f"✅ Features saved to 'features/engineered_features.csv'")
    print(f"Process completed in {end_time - start_time:.2f} seconds.")

# Run the script
if __name__ == '__main__':
    main()
