import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report, accuracy_score, confusion_matrix
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import GridSearchCV
import joblib
import os

# 📂 Paths
DATA_PATH = "features/preprocessed_data.csv"
MODEL_DIR = "src/models"
os.makedirs(MODEL_DIR, exist_ok=True)
MODEL_PATH = os.path.join(MODEL_DIR, "random_forest_classifier.pkl")
LABEL_ENCODER_PATH = os.path.join(MODEL_DIR, "label_encoder.pkl")

print("🔄 Loading data...")
df = pd.read_csv(DATA_PATH)

print("✂️ Splitting features and labels...")
X = df.drop(columns=["label"])
y_raw = df["label"]

# 🔠 Encode labels
label_encoder = LabelEncoder()
y = label_encoder.fit_transform(y_raw)

print("🔀 Splitting into train and test sets...")
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print("🚂 Training initial Random Forest model...")
clf = RandomForestClassifier(random_state=42)
clf.fit(X_train, y_train)

print("✅ Initial training done. Evaluating...")
y_pred = clf.predict(X_test)
print("📊 Classification Report:")
print(classification_report(y_test, y_pred, target_names=[str(cls) for cls in label_encoder.classes_], zero_division=0))
print("✅ Accuracy:", accuracy_score(y_test, y_pred))
print("🧾 Confusion Matrix:\n", confusion_matrix(y_test, y_pred))

print("🎯 Starting hyperparameter tuning...")
param_grid = {
    "n_estimators": [100, 200],
    "max_depth": [None, 10, 20],
    "min_samples_split": [2, 5],
    "min_samples_leaf": [1, 2],
}

grid_search = GridSearchCV(RandomForestClassifier(random_state=42), param_grid, cv=3, n_jobs=-1, verbose=1)
grid_search.fit(X_train, y_train)

print("✅ Best Parameters:", grid_search.best_params_)
best_model = grid_search.best_estimator_

print("🧪 Evaluating tuned model...")
y_pred_best = best_model.predict(X_test)
print("📊 Tuned Model Report:")
print(classification_report(y_test, y_pred_best, target_names=[str(cls) for cls in label_encoder.classes_], zero_division=0))
print("✅ Accuracy:", accuracy_score(y_test, y_pred_best))

print("💾 Saving model and label encoder...")
joblib.dump(best_model, MODEL_PATH)
joblib.dump(label_encoder, LABEL_ENCODER_PATH)
print(f"🎉 Model saved to: {MODEL_PATH}")
print(f"📚 Label encoder saved to: {LABEL_ENCODER_PATH}")
