# save_expected_features.py
import pandas as pd
import joblib

df = pd.read_csv("features/preprocessed_data.csv")
expected_features = df.drop(columns=["label"]).columns.tolist()

joblib.dump(expected_features, "src/models/expected_features.pkl")
print("✅ Saved expected feature names.")
