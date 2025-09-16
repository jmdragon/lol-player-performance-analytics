# src/train_win_model.py

import json, joblib
import pandas as pd
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score, accuracy_score, brier_score_loss

Path("artifacts").mkdir(exist_ok=True)
Path("reports").mkdir(exist_ok=True)

df = pd.read_parquet("data/model_table_simple.parquet")
# simple filter: only SR queues if you want (400/420/430/440)
df = df[df["champion"].notna()].copy()

X = df[["champion","role_clean","patch_minor","hour"]]
y = df["win"].astype(int)

pre = ColumnTransformer([
    ("cat", OneHotEncoder(handle_unknown="ignore"), ["champion","role_clean"]),
    ("num", "passthrough", ["patch_minor","hour"])
])

clf = Pipeline([
    ("prep", pre),
    ("model", LogisticRegression(max_iter=600))
])

Xtr, Xte, ytr, yte = train_test_split(X, y, test_size=0.25, random_state=42, stratify=y)
clf.fit(Xtr, ytr)
probs = clf.predict_proba(Xte)[:,1]
metrics = {
    "auc": float(roc_auc_score(yte, probs)),
    "acc@0.5": float(accuracy_score(yte, (probs>0.5).astype(int))),
    "brier": float(brier_score_loss(yte, probs)),
    "n_test": int(len(yte))
}
print(metrics)

joblib.dump(clf, "artifacts/win_model.joblib")
Path("reports/metrics.json").write_text(json.dumps(metrics, indent=2))
