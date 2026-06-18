"""Olist lakehouse dashboard (Phases 4 & 5).

Two audiences, two views — selectable in the sidebar:

  * **Pipeline Health** — operational metrics from warehouse/metadata.duckdb:
    run history, status, rows in/out/rejected, rejection rates, null-rate trends,
    and row-count deltas. This is the view most portfolio projects lack, and the
    one an engineer (or hiring manager) checks first to ask "is the pipeline OK?".
  * **Business** — KPIs and trends from the Gold marts in
    warehouse/lakehouse.duckdb: revenue, categories, states, top sellers,
    delivery performance, and customer (RFM) segments.

Run it:
    streamlit run app/streamlit_app.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pandas as pd
import plotly.express as px
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from config import settings  # noqa: E402

st.set_page_config(page_title="Olist Lakehouse", page_icon="📦", layout="wide")


# ── data access ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def query(db_path: str, sql: str) -> pd.DataFrame:
    """Run a read-only query and return a DataFrame (cached briefly)."""
    if not Path(db_path).exists():
        return pd.DataFrame()
    con = duckdb.connect(db_path, read_only=True)
    try:
        return con.sql(sql).df()
    finally:
        con.close()


def table_exists(db_path: str, schema: str, table: str) -> bool:
    if not Path(db_path).exists():
        return False
    df = query(
        db_path,
        f"SELECT 1 FROM information_schema.tables "
        f"WHERE table_schema='{schema}' AND table_name='{table}' LIMIT 1",
    )
    return not df.empty


META = str(settings.METADATA_DB)
LAKE = str(settings.LAKEHOUSE_DB)


# ── pipeline health view ─────────────────────────────────────────────────────
def render_pipeline_health() -> None:
    st.title("🩺 Pipeline Health")
    st.caption("Operational metrics — *is the pipeline working?* — from metadata.duckdb")

    if not table_exists(META, "main", "pipeline_runs"):
        st.warning("No pipeline metadata yet. Run: `python -m scripts.run_pipeline all`")
        return

    runs = query(META, "SELECT * FROM pipeline_runs")
    runs["started_at"] = pd.to_datetime(runs["started_at"])

    # headline KPIs
    total = len(runs)
    failed = int((runs["status"] == "failed").sum())
    rejected = int(runs["rows_rejected"].fillna(0).sum())
    last = runs["started_at"].max()
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total runs", f"{total:,}")
    c2.metric("Failed runs", f"{failed:,}")
    c3.metric("Rows quarantined", f"{rejected:,}")
    c4.metric("Last run", last.strftime("%Y-%m-%d %H:%M") if pd.notna(last) else "—")

    # latest run per pipeline/table
    st.subheader("Latest run per table")
    latest = query(
        META,
        """
        SELECT pipeline, table_name, status, rows_in, rows_out, rows_rejected,
               row_count_delta AS delta, round(duration_sec, 3) AS secs, started_at
        FROM (SELECT *, row_number() OVER
                (PARTITION BY pipeline, table_name ORDER BY started_at DESC) rn
              FROM pipeline_runs)
        WHERE rn = 1 ORDER BY pipeline, table_name
        """,
    )
    st.dataframe(latest, use_container_width=True, hide_index=True)

    col_a, col_b = st.columns(2)

    # rejection rate by table (silver)
    with col_a:
        st.subheader("Rejection rate by table (Silver)")
        rej = query(
            META,
            """
            SELECT table_name,
                   sum(rows_rejected) AS rejected,
                   sum(rows_in)       AS rows_in,
                   round(100.0 * sum(rows_rejected) / nullif(sum(rows_in), 0), 2) AS reject_pct
            FROM (SELECT *, row_number() OVER
                    (PARTITION BY table_name ORDER BY started_at DESC) rn
                  FROM pipeline_runs WHERE pipeline='silver')
            WHERE rn = 1
            GROUP BY table_name HAVING sum(rows_rejected) > 0 ORDER BY reject_pct DESC
            """,
        )
        if rej.empty:
            st.info("No rejected rows in the latest Silver runs. 🎉")
        else:
            st.plotly_chart(
                px.bar(rej, x="table_name", y="reject_pct",
                       labels={"reject_pct": "% rejected", "table_name": ""}),
                use_container_width=True,
            )

    # row-count delta vs previous run (drift signal)
    with col_b:
        st.subheader("Row-count delta vs previous run")
        st.caption("A large negative delta = something may have broken upstream.")
        delta = runs.dropna(subset=["row_count_delta"]).copy()
        if delta.empty:
            st.info("Deltas appear from the second run onward.")
        else:
            delta["label"] = delta["pipeline"] + "/" + delta["table_name"]
            st.plotly_chart(
                px.bar(delta.sort_values("started_at").tail(20),
                       x="label", y="row_count_delta",
                       labels={"row_count_delta": "Δ rows", "label": ""}),
                use_container_width=True,
            )

    # null-rate trends from column_metrics
    if table_exists(META, "main", "column_metrics"):
        st.subheader("Null rate by column (latest Silver run)")
        st.caption("A spike here is the earliest sign an upstream column changed.")
        nulls = query(
            META,
            """
            WITH latest AS (
                SELECT run_id FROM (
                    SELECT run_id, table_name, row_number() OVER
                        (PARTITION BY table_name ORDER BY created_at DESC) rn
                    FROM column_metrics WHERE pipeline='silver'
                ) WHERE rn = 1
            )
            SELECT table_name, column_name, round(null_rate, 3) AS null_rate
            FROM column_metrics
            WHERE run_id IN (SELECT run_id FROM latest) AND null_rate > 0
            ORDER BY null_rate DESC LIMIT 25
            """,
        )
        if nulls.empty:
            st.info("No nulls in key columns of the latest Silver run.")
        else:
            st.plotly_chart(
                px.bar(nulls, x="null_rate", y="column_name", color="table_name",
                       orientation="h", labels={"null_rate": "null rate", "column_name": ""}),
                use_container_width=True,
            )


# ── business view ────────────────────────────────────────────────────────────
def render_business() -> None:
    st.title("📊 Business Overview")
    st.caption("Revenue, delivery, and customer insights from the Gold marts")

    if not table_exists(LAKE, "gold", "fct_orders"):
        st.warning("Gold marts not built yet. Run: `python -m scripts.run_pipeline all`")
        return

    # headline KPIs
    kpis = query(
        LAKE,
        """
        SELECT
            (SELECT sum(order_revenue) FROM gold.fct_orders
             WHERE order_status NOT IN ('canceled','unavailable'))            AS revenue,
            (SELECT count(*) FROM gold.fct_orders)                            AS n_orders,
            (SELECT round(avg(days_to_deliver), 1) FROM gold.fct_orders
             WHERE days_to_deliver IS NOT NULL)                               AS avg_days,
            (SELECT round(100.0 * avg(case when delivered_on_time then 1.0 else 0.0 end), 1)
             FROM gold.fct_orders WHERE delivered_on_time IS NOT NULL)        AS on_time_pct
        """,
    ).iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Revenue (R$)", f"{(kpis['revenue'] or 0):,.0f}")
    c2.metric("Orders", f"{int(kpis['n_orders'] or 0):,}")
    c3.metric("Avg days to deliver", f"{kpis['avg_days'] or 0:.1f}")
    c4.metric("On-time delivery", f"{kpis['on_time_pct'] or 0:.1f}%")

    # revenue over time
    st.subheader("Monthly revenue")
    rev = query(
        LAKE,
        "SELECT year_month, sum(revenue) AS revenue FROM gold.mart_monthly_revenue "
        "GROUP BY 1 ORDER BY 1",
    )
    if not rev.empty:
        st.plotly_chart(
            px.line(rev, x="year_month", y="revenue", markers=True,
                    labels={"year_month": "", "revenue": "Revenue (R$)"}),
            use_container_width=True,
        )

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("Revenue by category (top 10)")
        cat = query(
            LAKE,
            "SELECT product_category, sum(revenue) AS revenue FROM gold.mart_monthly_revenue "
            "GROUP BY 1 ORDER BY revenue DESC LIMIT 10",
        )
        if not cat.empty:
            st.plotly_chart(
                px.bar(cat, x="revenue", y="product_category", orientation="h",
                       labels={"revenue": "Revenue (R$)", "product_category": ""}),
                use_container_width=True,
            )
    with col_b:
        st.subheader("Revenue by state (top 10)")
        state = query(
            LAKE,
            "SELECT customer_state, sum(revenue) AS revenue FROM gold.mart_monthly_revenue "
            "WHERE customer_state IS NOT NULL GROUP BY 1 ORDER BY revenue DESC LIMIT 10",
        )
        if not state.empty:
            st.plotly_chart(
                px.bar(state, x="customer_state", y="revenue",
                       labels={"revenue": "Revenue (R$)", "customer_state": ""}),
                use_container_width=True,
            )

    col_c, col_d = st.columns(2)
    with col_c:
        st.subheader("Customer segments (RFM)")
        seg = query(
            LAKE,
            "SELECT segment, count(*) AS customers, round(sum(monetary),2) AS monetary "
            "FROM gold.mart_customer_rfm GROUP BY 1 ORDER BY customers DESC",
        )
        if not seg.empty:
            st.plotly_chart(
                px.pie(seg, names="segment", values="customers", hole=0.4),
                use_container_width=True,
            )
    with col_d:
        st.subheader("Top sellers by revenue")
        sellers = query(
            LAKE,
            "SELECT seller_id, seller_state, total_revenue, n_orders, "
            "round(on_time_rate,2) AS on_time_rate, round(avg_review_score,2) AS review "
            "FROM gold.mart_seller_performance WHERE total_revenue IS NOT NULL "
            "ORDER BY total_revenue DESC LIMIT 10",
        )
        st.dataframe(sellers, use_container_width=True, hide_index=True)

    # delivery performance
    st.subheader("Delivery performance over time")
    deliv = query(
        LAKE,
        "SELECT year_month, sum(n_orders) AS orders, "
        "round(avg(on_time_rate),3) AS on_time_rate, round(avg(avg_days_to_deliver),1) AS avg_days "
        "FROM gold.mart_delivery_performance GROUP BY 1 ORDER BY 1",
    )
    if not deliv.empty:
        st.plotly_chart(
            px.line(deliv, x="year_month", y=["on_time_rate"], markers=True,
                    labels={"year_month": "", "value": "On-time rate"}),
            use_container_width=True,
        )


# ── shell ────────────────────────────────────────────────────────────────────
def main() -> None:
    st.sidebar.title("📦 Olist Lakehouse")
    view = st.sidebar.radio("View", ["Pipeline Health", "Business"])
    st.sidebar.markdown("---")
    st.sidebar.caption(
        "Bronze → Silver → Gold lakehouse. The **Pipeline Health** view reads the "
        "operational metadata store; **Business** reads the dbt Gold marts."
    )
    if view == "Pipeline Health":
        render_pipeline_health()
    else:
        render_business()


if __name__ == "__main__":
    main()
