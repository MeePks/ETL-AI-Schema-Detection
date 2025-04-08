import pickle

# Load the saved LabelEncoder
with open("src/models/label_encoder.pkl", "rb") as f:
    label_encoder = pickle.load(f)