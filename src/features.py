# src/features.py

import pandas as pd

pp = pd.read_parquet("data/participants_latest.parquet")
pm = pd.read_parquet("data/matches_latest.parquet")[["match_id","game_creation","game_version","queue"]]

df = pp.merge(pm, on="match_id", how="left")
df["game_creation"] = pd.to_datetime(df["game_creation"], utc=True)
df["hour"] = df["game_creation"].dt.tz_convert("America/New_York").dt.hour
# patch minor (safe parse)
ver = df["game_version"].fillna("0.0")
df["patch_minor"] = pd.to_numeric(ver.str.split(".").str[1], errors="coerce").fillna(0).astype(int)
df["role_clean"] = df["role"].fillna(df["lane"]).fillna("UNKNOWN")
df["win"] = df["win"].astype(int)

cols = ["match_id","summoner_name","win","champion","role_clean","patch_minor","hour","queue"]
out = df[cols].dropna(subset=["champion"])

out.to_parquet("data/model_table_simple.parquet", index=False)
print("Saved data/model_table_simple.parquet", out.shape)
