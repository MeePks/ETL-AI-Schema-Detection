import string

def extract_delimiter_features(text):
    features = {}
    for char in string.printable:
        features[f'{char}_count'] = text.count(char)
    return features