import pandas as pd

# Example dataset
data = {
    "Column1": ["John", "Doe", "Alice"],
    "Column2": [29, 35, 22],
    "Column3": ["2023-11-01", "2023-11-02", "2023-11-03"]
}
df = pd.DataFrame(data)

# Label data types
labels = ["string", "integer", "date"]
