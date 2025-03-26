import os
import sys
import joblib
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.feature_extraction import extract_delimiter_features

def detect_delimiter(file_path, max_lines=100):
    with open(file_path, 'r') as file:
        lines = [file.readline() for _ in range(max_lines)]
        text = ''.join(lines)
    
    features = extract_delimiter_features(text)
    model = joblib.load('src/models/delimiter_model.pkl')
    predicted_delimiter = model.predict([list(features.values())])
    
    return predicted_delimiter[0]

# Example usage
if __name__ == "__main__":
    file_path = 'Datasets\CSVDelimiter.txt'  # Replace with your input file path
    max_lines = 100  # Specify the number of lines to read
    delimiter = detect_delimiter(file_path, max_lines)
    print(f"Detected delimiter: {delimiter}")

