import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.ensemble import IsolationForest

# Load dataset (replace with your actual data file)
df = pd.read_csv("features/engineered_features.csv")

# Step 1: Replace empty or NaN values with the most frequent (mode) value
def fill_missing_values(df):
    imputer = SimpleImputer(strategy='most_frequent')  # Replace NaN with mode (most frequent value)
    df_filled = pd.DataFrame(imputer.fit_transform(df), columns=df.columns)
    return df_filled

df = fill_missing_values(df)

# Step 2: Encode categorical columns (Label Encoding or One-Hot Encoding)
def encode_categorical(df):
    label_encoder = LabelEncoder()

    # Apply label encoding to categorical columns (Example: if columns contain text or categorical data)
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = label_encoder.fit_transform(df[col].astype(str))

    return df

df = encode_categorical(df)

# Step 3: Handle outliers using Isolation Forest
def handle_outliers(df):
    iso_forest = IsolationForest(contamination=0.05)  # Set contamination rate (adjust accordingly)
    outliers = iso_forest.fit_predict(df.select_dtypes(include=[np.number]))  # Only numerical columns
    df = df[outliers == 1]  # Keep only the non-outlier data
    return df

df = handle_outliers(df)

# Step 4: Normalize/Scale numerical data
def scale_numerical_data(df):
    scaler = StandardScaler()  # Standardize features (zero mean, unit variance)
    
    # Identify numerical columns for scaling
    numerical_cols = df.select_dtypes(include=[np.number]).columns
    df[numerical_cols] = scaler.fit_transform(df[numerical_cols])
    
    return df

df = scale_numerical_data(df)

# Step 5: Remove columns with low variance (if any)
def remove_low_variance_columns(df):
    variance = df.var()
    low_variance_cols = variance[variance == 0].index
    df = df.drop(columns=low_variance_cols)
    return df

df = remove_low_variance_columns(df)

# Step 6: Save the preprocessed dataset
df.to_csv("features/preprocessed_data.csv", index=False)
print("Data preprocessing complete. Preprocessed data saved as 'preprocessed_data.csv'.")
