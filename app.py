from flask import Flask, request, jsonify
import os
import pandas as pd
from etlmodule import process_file  # Import your ETL logic

app = Flask(__name__)

# Define upload folder
UPLOAD_FOLDER = './uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def home():
    return "Welcome to the ETL API. Please use '/upload' to upload files."

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part in the request"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    # Save the file to the upload folder
    file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
    file.save(file_path)

    # Process the file
    try:
        schema = process_file(file_path)  # Your schema inference and ETL logic
        return jsonify({"message": "File processed successfully", "schema": schema}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({"status": "API is running"}), 200

if __name__ == '__main__':
    app.run(debug=True)
