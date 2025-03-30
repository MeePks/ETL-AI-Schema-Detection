import string

def extract_delimiter_features(text):
    features = {}
    for char in string.punctuation.replace('"','') + string.whitespace:
        count = text.count(char)
        features[f'{char}_count'] = count
        print(f"Character: {char}, Count: {count}")  # Debugging line
    return features

