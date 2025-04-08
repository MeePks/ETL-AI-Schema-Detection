# ML-Powered ETL Pipeline: Schema Inference and Data Loader

## Features
- **Automated Schema Detection**: Uses machine learning to detect file delimiters, headers, and column data types.
- **Dynamic Table Creation**: Automatically generates SQL Server tables based on detected schema.
- **Data Loading**: Efficiently loads data into SQL Server with error handling and logging.
- **AI-Driven Enhancements**: Implements machine learning for intelligent data type inference.

## Project Architecture
```plaintext
 +--------------------+         +--------------------+
 |  Delimited Files   |         |      ML Model      |
 +--------------------+         +--------------------+
           |                            |
           v                            v
 +--------------------+         +--------------------+
 | Feature Extraction |         | Schema Prediction |
 +--------------------+         +--------------------+
           |                            |
           +-----------> Data Schema <-----------+
                               |
                               v
                      +----------------+
                      | SQL Server DB  |
                      +----------------+
