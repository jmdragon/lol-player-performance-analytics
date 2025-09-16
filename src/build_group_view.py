# src/build_group_view.py

import pandas as pd
from pathlib import Path

DATA = Path("data")

# Load
pp = pd.read_parquet(DATA / "participants_latest.parquet")
pm = pd.read_parquet(DATA / "matches_latest.parquet")[["match_id","game_creation","queue","game_version"]]
roster = pd.read_csv(DATA / "roster.csv")  # columns: riot_id, puuid (and maybe platform)

# Merge PUUID → Riot ID
df = pp.merge(pm, on="match_id", how="left").merge(roster[["puuid","riot_id"]], on="puuid", how="left")

# Pick a stable display label:
# Prefer your roster Riot ID (e.g., "Ikkyro#NA1") if present; otherwise fall back to API summoner_name
def choose_label(row):
    rid = row.get("riot_id")
    sname = row.get("summoner_name")
    if isinstance(rid, str) and len(rid.strip()) > 0:
        return rid
    if isinstance(sname, str) and len(sname.strip()) > 0:
        return sname
    return "(unknown)"

df["player_label"] = df.apply(choose_label, axis=1)

# Mark whether the row is one of "your group" (came from roster)
df["in_group"] = df["riot_id"].notna()

# Type fixes
df["game_creation"] = pd.to_datetime(df["game_creation"], errors="coerce", utc=True)
if df["win"].dtype != "int64" and df["win"].dtype != "Int64":
    df["win"] = df["win"].astype(bool).astype(int)

# Save an enriched version for the app
out_cols = [
    "match_id","puuid","riot_id","player_label","in_group","summoner_name","champion","role","lane","win",
    "kills","deaths","assists","cs","gold","vision_score","damage_dealt","time_ccing",
    "game_creation","game_version","queue"
]
df[out_cols].to_parquet(DATA / "participants_group_latest.parquet", index=False)
print("Saved:", DATA / "participants_group_latest.parquet", "rows:", len(df))

