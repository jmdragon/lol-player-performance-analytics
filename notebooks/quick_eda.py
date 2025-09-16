import pandas as pd
from pathlib import Path

pm = pd.read_parquet("data/matches_latest.parquet")
pp = pd.read_parquet("data/participants_latest.parquet")

# Basic derived metrics
pp["kda"] = (pp["kills"] + pp["assists"]) / pp["deaths"].replace(0, 1)
pp["cs"] = pp["cs"].fillna(0)
pm["game_creation"] = pd.to_datetime(pm["game_creation"], utc=True)
pp = pp.merge(pm[["match_id","game_creation","game_version","queue"]], on="match_id", how="left")

# Per-player summary
player = (pp.groupby("summoner_name")
            .agg(games=("win","size"),
                 winrate=("win","mean"),
                 kda=("kda","mean"))
            .sort_values("games", ascending=False))

# Top champions per player (min 8 games)
champ = (pp.groupby(["summoner_name","champion"])
           .agg(games=("win","size"),
                winrate=("win","mean"),
                kda=("kda","mean"))
           .query("games >= 8")
           .sort_values(["summoner_name","winrate"], ascending=[True, False]))

Path("reports").mkdir(exist_ok=True)
player.to_csv("reports/player_summary.csv")
champ.reset_index().to_csv("reports/top_champs_by_player.csv", index=False)

print("Saved reports/player_summary.csv and reports/top_champs_by_player.csv")
