import pandas as pd
import numpy as np
import re
import joblib
from sklearn.preprocessing import StandardScaler
from sklearn.impute import SimpleImputer
from dateutil.parser import parse

# Load trained artifacts
#print("Loading model...")
model = joblib.load("src/models/random_forest_classifier.pkl")

#print("Loading label encoder...")
label_encoder = joblib.load("src/models/label_encoder.pkl")

#print("Loading expected features...")
with open("src/models/expected_features.pkl", "rb") as f:
    expected_features = joblib.load(f)

#print(f"Expected features loaded: {expected_features[:5]}...")  # Print the first few expected features for debugging

# --------------------------------
# Step 1: Feature extraction logic
# --------------------------------
def is_number(s):
    try:
        float(s)
        return True
    except:
        return False

def is_date(string):
    try:
        parse(string)
        return True
    except:
        return False

def extract_features_from_column(column,n=10000):
    col_data = column.dropna().astype(str)
    total = len(col_data)
    
    if total == 0:
        return {
            'length': 0,
            'num_digits': 0,
            'num_letters': 0,
            'num_special_chars': 0,
            'contains_alpha': 0,
            'contains_digit': 0,
            'contains_date_sep': 0,
            'is_float': 0,
            'is_int': 0,
            'num_tokens': 0,
            'is_uppercase': 0,
            'percent_numeric': 0,
            'percent_alpha': 0,
            'percent_alnum': 0,
            'percent_special': 0,
            'percent_date': 0,
            'avg_length': 0,
            'max_length': 0
        }

    vals = col_data.head(n).tolist()
    features = {
        'length': np.mean([len(val) for val in vals]),
        'num_digits': np.mean([sum(c.isdigit() for c in val) for val in vals]),
        'num_letters': np.mean([sum(c.isalpha() for c in val) for val in vals]),
        'num_special_chars': np.mean([sum(not c.isalnum() for c in val) for val in vals]),
        'contains_alpha': np.mean([any(c.isalpha() for c in val) for val in vals]),
        'contains_digit': np.mean([any(c.isdigit() for c in val) for val in vals]),
        'contains_date_sep': np.mean([bool(re.search(r"[/-]", val)) for val in vals]),
        'is_float': np.mean([is_number(val) for val in vals]),
        'is_int': np.mean([is_number(val) and float(val).is_integer() for val in vals]),
        'num_tokens': np.mean([len(val.split()) for val in vals]),
        'is_uppercase': np.mean([val.isupper() for val in vals]),
        'percent_numeric': np.mean([is_number(val) for val in vals]),
        'percent_alpha': np.mean([val.isalpha() for val in vals]),
        'percent_alnum': np.mean([val.isalnum() for val in vals]),
        'percent_special': np.mean([bool(re.search(r'[^a-zA-Z0-9]', val)) for val in vals]),
        'percent_date': np.mean([is_date(val) for val in vals]),
        'avg_length': np.mean([len(val) for val in vals]),
        'max_length': max([len(val) for val in vals])
    }
    return features

def map_numeric_to_datatype(prediction):
    # Map numeric labels (1, 2, 3, etc.) to actual data types
    mapping = {
        4: "varchar",   # For example: 0 -> varchar
        2: "float",     # 1 -> float
        1: "date",      # 2 -> date
        3: "int",       # 3 -> int
        5: "boolean",   # 4 -> boolean (example, adjust based on your model)
        # Add more mappings if necessary
    }
    
    return mapping.get(prediction, "unknown") 

# --------------------------------
# Step 2: Predict data types
# --------------------------------
def predict_schema(file_path,delimiter=",",nrows=1000,has_header=True):
    #print(f"Loading data from: {file_path}")
    if has_header:
        raw_df = pd.read_csv(file_path, delimiter=delimiter, nrows=10000)
    else:
        raw_df = pd.read_csv(file_path, delimiter=delimiter, header=None, nrows=10000)
        raw_df.columns = [f"field{i+1}" for i in range(raw_df.shape[1])]

    #print(f"Extracting features from {len(raw_df.columns)} columns...")
    feature_rows = []
    raw_feature_data = []  # List to store raw features for each column
    for col in raw_df.columns:
        features = extract_features_from_column(raw_df[col], n=nrows)
        feature_rows.append(features)
        #feature_rows.append(extract_features_from_column(raw_df[col], n=nrows))
        raw_feature_data.append(features)  # Store raw feature data

    features_df = pd.DataFrame(feature_rows)
    #print(f" Extracted features shape: {features_df.shape}")

    #print("🧼 Aligning features with expected ones...")
    for col in expected_features:
        if col not in features_df.columns:
            features_df[col] = 0
    features_df = features_df[expected_features]
    #print(f" Final feature shape: {features_df.shape}")

    # Fill missing values
    imputer = SimpleImputer(strategy='most_frequent')
    features_df = pd.DataFrame(imputer.fit_transform(features_df), columns=features_df.columns)
    #print(f" Missing values filled. Feature shape: {features_df.shape}")

    # Scale features
    scaler = StandardScaler()
    features_df[features_df.columns] = scaler.fit_transform(features_df)
    #print(f" Features scaled.")

    print("Predicting data types...")

    # Predict using the model
    predictions = model.predict(features_df)

    # Check if LabelEncoder is already fitted, if not, fit it on the model's labels
    if not hasattr(label_encoder, 'classes_'):
        label_encoder.fit(model.classes_)

    # Map numeric predictions to actual data types using the custom function
    predicted_labels = [map_numeric_to_datatype(label) for label in predictions]

    # Print out the unique predicted labels for debugging
    #print(f"Predicted labels (mapped): {predicted_labels}")


    # Create the result DataFrame
    result_df = pd.DataFrame({
        "column_name": raw_df.columns,
        "predicted_data_type": predicted_labels,
    })

    # Add the raw feature data (like max_length) into the result DataFrame
    for idx, column in enumerate(raw_df.columns):
        result_df.loc[idx, "max_length"] = int(raw_feature_data[idx]["max_length"])

    result_df["max_length"] = result_df["max_length"].astype(int)

    print("\nPrediction result:\n")
    print(result_df)


# --------------------------------
# Run the prediction
# --------------------------------
if __name__ == "__main__":
    predict_schema(r"Y:\Data\Retail\WalmartMX\Development\Pikesh.Maharjan\ETL-AI-Schema-Detection\data\samples\file_26.dat",'^',1000)  # Change path as needed
