import pandas as pd
import re
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split


# Example dataset
data = {
    "Column1": ["John", "Doe", "Alice"],
    "Column2": [29, 35, 22],
    "Column3": ["2023-11-01", "2023-11-02", "2023-11-03"]
}
df = pd.DataFrame(data)

# Label data types
labels = ["string", "integer", "date"]

def extract_features(column):
    # Initialize feature dictionary
    features = {}
    
    # Statistical features
    features["is_numeric"] = column.apply(lambda x: str(x).replace('.', '', 1).isdigit()).mean()
    features["mean_length"] = column.apply(lambda x: len(str(x))).mean()
    
    # Regex pattern matching
    features["contains_date"] = column.apply(lambda x: bool(re.match(r'\d{4}-\d{2}-\d{2}', str(x)))).mean()
    
    return pd.Series(features)

# Apply feature extraction
features = df.apply(extract_features)
print(features)


# Prepare training data
X = features.values
y = labels  # Data types

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train model
model = RandomForestClassifier()
model.fit(X_train, y_train)

# Predict on new data
predictions = model.predict(X_test)
print("Predicted Data Types:",X_test,predictions)
