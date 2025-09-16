import streamlit as st
import pandas as pd
import numpy as np
import io
import altair as alt

st.set_page_config(page_title="LoL Group Dashboard", layout="wide")

# ---------- Data loaders ----------
@st.cache_data
def load_group():
    df = pd.read_parquet("data/participants_group_latest.parquet")
    # types & time
    df["game_creation"] = pd.to_datetime(df["game_creation"], errors="coerce", utc=True)
    try:
        df["game_creation"] = df["game_creation"].dt.tz_convert("America/New_York")
    except Exception:
        pass
    if df["win"].dtype not in ("int64", "Int64"):
        df["win"] = df["win"].astype(bool).astype(int)
    df["hour"] = df["game_creation"].dt.hour
    df["date"] = df["game_creation"].dt.date
    return df

df = load_group()
if df.empty:
    st.error("No data found. Make sure you ran the ETL and build_group_view.py.")
    st.stop()

# ---------- Sidebar filters ----------
with st.sidebar:
    st.markdown("### Filters")
    scope = st.radio("Scope", ["My group only", "All players"], index=0)
    queues_all = sorted(df["queue"].dropna().unique().tolist())
    sel_queues = st.multiselect("Queues (400=Normal Draft, 420=Ranked Solo)", queues_all, default=queues_all)

    # Date range
    min_d, max_d = df["date"].min(), df["date"].max()
    start_d, end_d = st.date_input("Date range", value=(min_d, max_d))
    if isinstance(start_d, tuple):  # rare first-render quirk
        start_d, end_d = start_d

    # Rolling window for trend chart
    window = st.slider("Rolling window (games)", min_value=5, max_value=30, value=10, step=1)

    # Min games for champ tables
    min_games = st.number_input("Min games per champ", 1, 100, 5)

# ---------- Apply global filters ----------
sub = df.copy()
if scope == "My group only":
    sub = sub[sub["in_group"] == True]
if sel_queues:
    sub = sub[sub["queue"].isin(sel_queues)]
sub = sub[(sub["date"] >= start_d) & (sub["date"] <= end_d)]

if sub.empty:
    st.warning("No rows after filters. Try broadening the date range, queues, or scope.")
    st.dataframe(df[["player_label", "summoner_name", "queue", "win"]].head(20))
    st.stop()

# ---------- Overview ----------
oc1, oc2, oc3 = st.columns(3)
oc1.metric("Matches", f"{sub['match_id'].nunique():,}")
oc2.metric("Player-games", f"{len(sub):,}")
oc3.metric("Win rate", f"{100*sub['win'].mean():.1f}%")

# Download filtered rows
csv_buf = io.StringIO()
sub.to_csv(csv_buf, index=False)
st.download_button("Download filtered rows (CSV)", csv_buf.getvalue(), "filtered_rows.csv")

st.markdown("---")

# ---------- Tabs ----------
tab1, tab2, tab3, tab4 = st.tabs(["Player detail", "Pick Advisor", "Champions overview", "Contribution"])

# ===== Player Detail =====
with tab1:
    players = sorted(sub["player_label"].dropna().unique().tolist())
    p = st.selectbox("Player", players)
    p_df = sub[sub["player_label"] == p].sort_values("game_creation")

    c1, c2, c3 = st.columns(3)
    c1.metric("Games", len(p_df))
    c2.metric("Win rate", f"{100*p_df['win'].mean():.1f}%")
    kda = (p_df["kills"] + p_df["assists"]) / p_df["deaths"].replace(0, 1)
    c3.metric("Avg KDA", f"{kda.mean():.2f}")

    # Rolling trend
    if p_df["game_creation"].notna().sum() > 0:
        roll = (
            p_df.assign(win_int=p_df["win"].astype(int))
                .set_index("game_creation")["win_int"]
                .rolling(window, min_periods=1).mean()
        )
        st.line_chart(roll)
    else:
        st.info("No timestamps available to draw a rolling chart.")

# ===== Pick Advisor (Champion Lift) =====
with tab2:
    base = sub.groupby("player_label")["win"].mean().rename("base")
    champ_tbl = (
        sub.groupby(["player_label", "champion"])
           .agg(games=("win", "size"), winrate=("win", "mean"))
           .reset_index()
    ).merge(base, on="player_label")
    champ_tbl["lift"] = champ_tbl["winrate"] - champ_tbl["base"]
    champ_tbl = champ_tbl[champ_tbl["games"] >= int(min_games)]

    pt = champ_tbl[champ_tbl["player_label"] == p].copy()
    pt = pt.sort_values("lift", ascending=False)
    st.markdown("**Champion suggestions (relative to your baseline)**")
    st.dataframe(pt[["champion", "games", "winrate", "base", "lift"]])

    csv_buf2 = io.StringIO()
    pt.to_csv(csv_buf2, index=False)
    st.download_button("Download pick advisor (CSV)", csv_buf2.getvalue(), f"pick_advisor_{p}.csv")

# ===== Champions overview (group-level) =====
with tab3:
    gtbl = (
        sub.groupby("champion")
           .agg(games=("win", "size"), winrate=("win", "mean"))
           .sort_values(["games", "winrate"], ascending=[False, False])
           .reset_index()
    )
    st.dataframe(gtbl)

# ===== Contribution (player impact on winrate) =====
with tab4:
    st.markdown("### Contribution Scores")
    st.caption(
        "Winrate WITH player − winrate in games WITHOUT that player (within current filters). "
        "Weighted score multiplies by √games to reduce small-sample noise."
    )

    # Controls specific to this tab
    ccol1, ccol2, ccol3 = st.columns([1,1,1])
    min_games_contrib = ccol1.number_input("Min games (contrib)", 1, 200, 5)
    use_weight = ccol2.toggle("Use weighted score (× √games)", value=True)
    show_table = ccol3.toggle("Show raw table", value=False)

    # ---- Compute with/without table ----
    scores = []
    uniq_players = sorted(sub["player_label"].dropna().unique().tolist())
    for pl in uniq_players:
        with_pl = sub[sub["player_label"] == pl]
        g = len(with_pl)
        if g == 0:
            continue

        mids_with = set(with_pl["match_id"].unique())
        wr_with = with_pl["win"].mean()

        # Baseline: all games excluding matches containing this player
        without_mask = ~sub["match_id"].isin(mids_with)
        wr_without = sub.loc[without_mask, "win"].mean() if without_mask.any() else np.nan

        contrib = (wr_with - wr_without) if pd.notna(wr_with) and pd.notna(wr_without) else np.nan
        scores.append({
            "player": pl,
            "games": g,
            "winrate_with": wr_with,
            "winrate_without": wr_without,
            "contribution": contrib,
        })

    contrib = pd.DataFrame(scores).dropna(subset=["contribution"])
    contrib = contrib[contrib["games"] >= int(min_games_contrib)].copy()

    if contrib.empty:
        st.warning("No players pass the min-games threshold for contribution.")
        st.stop()

    # Weighted score + sorting
    contrib["weighted_contribution"] = contrib["contribution"] * np.sqrt(contrib["games"])
    sort_key = "weighted_contribution" if use_weight else "contribution"
    contrib = contrib.sort_values(sort_key, ascending=False).reset_index(drop=True)

    # ---------- NEW: Winrate WITH vs WITHOUT (grouped bars + baseline) ----------
    st.markdown("#### Winrate with vs without each player")

    # Long format for grouped bars
    wr_long = (
        contrib[["player", "games", "winrate_with", "winrate_without"]]
        .melt(id_vars=["player","games"], var_name="type", value_name="winrate")
        .replace({"type": {"winrate_with": "With player", "winrate_without": "Without player"}})
    )

    # Keep player order consistent with the impact sort
    wr_long["player"] = pd.Categorical(wr_long["player"], categories=contrib["player"], ordered=True)

    overall_wr = float(sub["win"].mean())

    # Grouped bars in a single plot (works best in Streamlit)
    wr_chart = (
        alt.Chart(wr_long)
        .mark_bar()
        .encode(
            x=alt.X("player:N", sort=list(contrib["player"]), title=None, axis=alt.Axis(labelLimit=180)),
            y=alt.Y("winrate:Q", title="Win rate", axis=alt.Axis(format="%")),
            color=alt.Color("type:N", title=None),
            xOffset="type:N",
            tooltip=[
                alt.Tooltip("player:N"),
                alt.Tooltip("games:Q"),
                alt.Tooltip("type:N"),
                alt.Tooltip("winrate:Q", format=".1%")
            ]
        )
        .properties(height=max(280, 24 * len(contrib)), width=900)
    )

    # Baseline rule layered on the same chart
    baseline = (
        alt.Chart(pd.DataFrame({"overall":[overall_wr]}))
        .mark_rule(strokeDash=[4,4])
        .encode(y=alt.Y("overall:Q", axis=None))
    )

    st.altair_chart(alt.layer(wr_chart, baseline), use_container_width=True)

    # ---------- Impact chart (weighted or raw contribution) ----------
    st.markdown("#### Impact score (contribution vs baseline)")
    chart_df = contrib[["player","games","contribution","weighted_contribution"]].copy()
    chart_df["score"] = chart_df["weighted_contribution"] if use_weight else chart_df["contribution"]
    max_abs = float(np.nanmax(np.abs(chart_df["score"]))) or 0.0
    if max_abs == 0.0: max_abs = 1.0

    impact_chart = (
        alt.Chart(chart_df)
           .mark_bar()
           .encode(
               x=alt.X("score:Q", title="Impact score" + (" (weighted)" if use_weight else "")),
               y=alt.Y("player:N", sort="-x", title=None),
               color=alt.Color("score:Q",
                               scale=alt.Scale(domain=[-max_abs, 0, max_abs], scheme="redblue"),
                               legend=None),
               tooltip=[
                   alt.Tooltip("player:N"),
                   alt.Tooltip("games:Q"),
                   alt.Tooltip("contribution:Q", format=".2%"),
                   alt.Tooltip("weighted_contribution:Q", format=".3f")
               ]
           )
           .properties(height=28 * len(chart_df), width=900)
    )
    st.altair_chart(impact_chart, use_container_width=True)

    # Optional raw table + downloads
    if show_table:
        st.dataframe(contrib)

    csv_buf3 = io.StringIO()
    contrib.to_csv(csv_buf3, index=False)
    st.download_button("Download contribution (CSV)", csv_buf3.getvalue(), "contribution.csv")
