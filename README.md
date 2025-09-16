# League of Legends Player Performance Analytics (Riot API)
Python ETL → Parquet → Streamlit dashboard for multi-player match analytics, champion pick advice, and contribution scoring.

**Tech:** Python, Pandas, Streamlit, Altair, scikit-learn, Cassiopeia (Riot API)

---

## What this does
- **Extract** match data from the Riot API (Riot IDs → PUUID → matches & details)
- **Transform** raw JSON into tidy Parquet tables (`matches`, `participants`, `participants_group`)
- **Load** into an interactive **Streamlit** dashboard with:
  - Per-player **win rate**, **rolling form** (last N games)
  - **Pick Advisor**: champion **lift** (champ win rate – player baseline)
  - **Contribution**: **with/without** win rate & **weighted** impact score (+ optional adjusted plus–minus)
  - Filters for **date range**, **queue (400/420)**, **min games**, etc.

---

## Project structure
- lol-ds/
- ├─ app/
- │ └─ app.py # Streamlit app
- ├─ src/
- │ ├─ init.py # Package marker
- │ ├─ etl_http_riot.py # Riot API ETL (HTTP)
- │ ├─ build_group_view.py # Merge participants+matches+roster → participants_group_latest.parquet
- │ ├─ features.py # Feature helpers (optional)
- │ └─ train_win_model.py # Baseline model (optional)
- ├─ data/ # (gitignored) parquet output lives here
- ├─ artifacts/ # (gitignored) trained models
- ├─ reports/ # (gitignored) CSV exports
- ├─ riot_ids.yaml # Your friends' Riot IDs (source list)
- ├─ requirements.txt # Lean runtime deps
- ├─ .env.example # Template env (no secrets)
- ├─ .gitignore
- └─ README.md

