import os
import pandas as pd
import sys
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer
import string
# Add the src directory to the system path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.feature_extraction import extract_delimiter_features

# Function to read sample files and extract delimiters
def read_sample_files(folder_path):
    data = []
    for file_name in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file_name)
        print(f"Reading file: {file_path}")
        with open(file_path, 'r') as file:
            text = file.read()
            delimiter = detect_delimiter_in_sample(text)
            data.append({'text': text, 'delimiter': delimiter})
    return pd.DataFrame(data)

# Function to detect delimiter in sample text (for training purposes)
def detect_delimiter_in_sample(text):
    # Consider only punctuation and special characters as potential delimiters
    potential_delimiters = string.punctuation.replace('"','') + string.whitespace
    
    # Count occurrences of each character
    delimiter_counts = {char: text.count(char) for char in potential_delimiters}
    
    # Find the character with the highest count
    most_frequent_delimiter = max(delimiter_counts, key=delimiter_counts.get)
    print(f"Detected delimiter: {most_frequent_delimiter}")
    
    # Return the most frequent character if its count is significant
    return most_frequent_delimiter if delimiter_counts[most_frequent_delimiter] > 1 else None

# Read sample files
folder_path = 'data/samples/'
df = read_sample_files(folder_path)

# Feature extraction
features = df['text'].apply(extract_delimiter_features).apply(pd.Series)
X = features
y = df['delimiter']

# Train-test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train the model
clf = RandomForestClassifier()
clf.fit(X_train, y_train)

# Evaluate the model
train_accuracy = clf.score(X_train, y_train)
test_accuracy = clf.score(X_test, y_test)
print(f"Train accuracy: {train_accuracy}")
print(f"Test accuracy: {test_accuracy}")

# Save the model
import joblib
joblib.dump(clf, 'src\models\delimiter_model.pkl')
