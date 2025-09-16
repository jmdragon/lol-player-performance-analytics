# src/etl_http_riot.py
# Direct Riot REST ETL: Riot ID -> PUUID -> Match IDs -> Match details -> Parquet

import os, time, math
from pathlib import Path
from datetime import datetime
import requests
import pandas as pd
import yaml
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("RIOT_API_KEY")
ROUTING = os.getenv("MATCH_ROUTING", "americas").lower()   
MAX_MATCHES = int(os.getenv("MAX_MATCHES_PER_PLAYER", "100"))
QUEUE = 400  # Draft Norms

if not API_KEY:
    raise RuntimeError("Missing RIOT_API_KEY in .env")

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

# ---------- HTTP helpers ----------
def riot_get(url, params=None, max_retries=5):
    """GET with Riot API key + 429 backoff."""
    headers = {"X-Riot-Token": API_KEY}
    for attempt in range(max_retries):
        r = requests.get(url, headers=headers, params=params, timeout=20)
        if r.status_code == 200:
            return r.json()
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", "2"))
            time.sleep(wait)
            continue
        if 500 <= r.status_code < 600:
            # transient
            time.sleep(1.5 * (attempt + 1))
            continue
        # hard error
        raise RuntimeError(f"GET {url} -> {r.status_code} {r.text}")
    raise RuntimeError(f"GET {url} failed after {max_retries} retries")

def resolve_puuid(game_name: str, tag_line: str) -> str:
    url = f"https://{ROUTING}.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{requests.utils.quote(game_name)}/{requests.utils.quote(tag_line)}"
    data = riot_get(url)
    return data["puuid"]

def get_match_ids(puuid: str, count: int, queue: int) -> list[str]:
    # Pull in batches of up to 100 (API limit)
    ids = []
    start = 0
    while len(ids) < count:
        need = min(100, count - len(ids))
        url = f"https://{ROUTING}.api.riotgames.com/lol/match/v5/matches/by-puuid/{puuid}/ids"
        params = {"start": start, "count": need, "queue": queue}
        batch = riot_get(url, params=params)
        if not batch:
            break
        ids.extend(batch)
        if len(batch) < need:
            break
        start += need
    return ids

def get_match_detail(match_id: str) -> dict:
    url = f"https://{ROUTING}.api.riotgames.com/lol/match/v5/matches/{match_id}"
    return riot_get(url)

# ---------- flattening ----------
def flatten_matches(match_jsons: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    m_rows, p_rows = [], []
    for mj in match_jsons:
        info = mj.get("info", {})
        meta = mj.get("metadata", {})
        match_id = meta.get("matchId")
        if not match_id:
            continue
        m_rows.append({
            "match_id": match_id,
            "game_version": info.get("gameVersion"),
            "game_creation": pd.to_datetime(info.get("gameStartTimestamp"), unit="ms", utc=True) if info.get("gameStartTimestamp") else None,
            "game_duration_s": info.get("gameDuration"),
            "queue": info.get("queueId"),
            "map_id": info.get("mapId"),
            "game_mode": info.get("gameMode"),
            "game_type": info.get("gameType"),
        })
        for p in info.get("participants", []):
            p_rows.append({
                "match_id": match_id,
                "puuid": p.get("puuid"),
                "summoner_name": p.get("summonerName"),
                "team_id": p.get("teamId"),
                "champion": p.get("championName"),
                "role": p.get("role"),
                "lane": p.get("lane"),
                "win": bool(p.get("win")),
                "kills": p.get("kills"),
                "deaths": p.get("deaths"),
                "assists": p.get("assists"),
                "total_minions_killed": p.get("totalMinionsKilled"),
                "neutral_minions_killed": p.get("neutralMinionsKilled"),
                "cs": (p.get("totalMinionsKilled") or 0) + (p.get("neutralMinionsKilled") or 0),
                "gold": p.get("goldEarned"),
                "vision_score": p.get("visionScore"),
                "damage_dealt": p.get("totalDamageDealtToChampions"),
                "time_ccing": p.get("timeCCingOthers"),
            })
    df_m = pd.DataFrame(m_rows).drop_duplicates("match_id")
    df_p = pd.DataFrame(p_rows)
    return df_m, df_p

# ---------- main ----------
def main():
    ypath = Path("riot_ids.yaml")
    if not ypath.exists():
        raise FileNotFoundError("riot_ids.yaml not found.")
    conf = yaml.safe_load(ypath.read_text())
    raw_players = conf.get("players", [])
    # support both plain strings "Name#Tag" and dicts {"id": "...", "platform": "..."} (platform unused in HTTP path)
    players = []
    for entry in raw_players:
        if isinstance(entry, str):
            players.append({"id": entry})
        elif isinstance(entry, dict) and "id" in entry:
            players.append(entry)
        else:
            continue

    roster = []
    all_match_ids = set()

    # Resolve PUUIDs
    for p in players:
        rid = p["id"]
        if "#" not in rid:
            print(f"[skip] Not a Riot ID (expect 'name#tag'): {rid}")
            continue
        name, tag = rid.split("#", 1)
        print(f"[resolve] {name}#{tag}")
        try:
            puuid = resolve_puuid(name.strip(), tag.strip())
            roster.append({"riot_id": rid, "puuid": puuid})
        except Exception as e:
            print(f"[warn] failed to resolve {rid}: {e}")

    # Fetch match ids and details
    match_jsons = []
    for r in roster:
        print(f"[fetch-ids] {r['riot_id']} last {MAX_MATCHES} (queue {QUEUE})")
        try:
            ids = get_match_ids(r["puuid"], MAX_MATCHES, QUEUE)
        except Exception as e:
            print(f"[warn] ids failed for {r['riot_id']}: {e}")
            continue
        pulled = 0
        for mid in ids:
            if mid in all_match_ids:
                continue
            try:
                mj = get_match_detail(mid)
                match_jsons.append(mj)
                all_match_ids.add(mid)
                pulled += 1
            except Exception as e:
                print(f"[warn] match {mid} failed: {e}")
        print(f"[done-ids] {r['riot_id']} unique pulled: {pulled}")

    print(f"[dedupe] unique matches collected: {len(match_jsons)}")

    df_m, df_p = flatten_matches(match_jsons)
    ts = datetime.now().strftime("%Y%m%d_%H%M")

    pd.DataFrame(roster).to_csv(DATA_DIR / "roster.csv", index=False)
    df_m.to_parquet(DATA_DIR / f"matches_{ts}.parquet", index=False)
    df_p.to_parquet(DATA_DIR / f"participants_{ts}.parquet", index=False)
    df_m.to_parquet(DATA_DIR / "matches_latest.parquet", index=False)
    df_p.to_parquet(DATA_DIR / "participants_latest.parquet", index=False)

    print(f"[saved] matches={len(df_m)} participants={len(df_p)} -> data/*.parquet")

if __name__ == "__main__":
    main()
