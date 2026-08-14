from __future__ import annotations

from datetime import timedelta

import pendulum

try:
    from airflow.sdk import dag
except ImportError:
    from airflow.decorators import dag

from helpers.hallolaundry import SQL_DIR, TIMEZONE, make_bigquery_sql_task


@dag(
    dag_id="build_haghi_customer_analytics",
    schedule="0 11 * * 0",  # Sunday 11:00 WIB, after weekly ingestion/datamart window
    start_date=pendulum.datetime(2026, 8, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[SQL_DIR],
    default_args={
        "owner": "data-engineering",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["haghi-laundry", "datamart", "customer-analytics", "rfm"],
)
def build_haghi_customer_analytics():
    # Reusable customer-service-order foundation.
    order_events = make_bigquery_sql_task(
        "mart_customer_order_events",
        "mart_customer_analytics/19_mart_customer_order_events.sql",
    )

    # Current customer-level RFM / lifecycle snapshot.
    customer_analytics = make_bigquery_sql_task(
        "mart_customer_analytics",
        "mart_customer_analytics/22_mart_customer_analytics.sql",
    )

    # Monthly customer activity at customer x month grain.
    monthly_activity = make_bigquery_sql_task(
        "mart_monthly_customer_activity",
        "mart_customer_analytics/25_mart_monthly_customer_activity.sql",
    )

    # Dashboard-ready monthly acquisition / retention / churn metrics.
    monthly_metrics = make_bigquery_sql_task(
        "mart_monthly_customer_metrics",
        "mart_customer_analytics/26_mart_monthly_customer_metrics.sql",
    )

    # Both downstream customer marts require the normalized order-event table.
    order_events >> [customer_analytics, monthly_activity]

    # Monthly metrics aggregate the customer x month activity table.
    monthly_activity >> monthly_metrics


build_haghi_customer_analytics()