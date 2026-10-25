import random
import time
from datetime import datetime

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule


def choose_medal():
    return random.choice(["Bronze", "Silver", "Gold"])


def select_medal_branch(ti):
    medal = ti.xcom_pull(task_ids="choose_medal")
    return f"count_{medal.lower()}"


def wait_five_seconds():
    time.sleep(5)


with DAG(
    dag_id="olympic_medal_dag",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["goit", "olympics"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="goit_mysql_db",
        sql="""
            CREATE TABLE IF NOT EXISTS medal_counts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(20) NOT NULL,
                `count` BIGINT NOT NULL,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
        """,
    )

    choose_medal_task = PythonOperator(
        task_id="choose_medal",
        python_callable=choose_medal,
    )

    branch_medal = BranchPythonOperator(
        task_id="branch_medal",
        python_callable=select_medal_branch,
    )

    count_bronze = SQLExecuteQueryOperator(
        task_id="count_bronze",
        conn_id="goit_mysql_db",
        sql="""
            INSERT INTO medal_counts (medal_type, `count`, created_at)
            SELECT 'Bronze', COUNT(*), CURRENT_TIMESTAMP
            FROM athlete_event_results
            WHERE medal = 'Bronze'
        """,
    )

    count_silver = SQLExecuteQueryOperator(
        task_id="count_silver",
        conn_id="goit_mysql_db",
        sql="""
            INSERT INTO medal_counts (medal_type, `count`, created_at)
            SELECT 'Silver', COUNT(*), CURRENT_TIMESTAMP
            FROM athlete_event_results
            WHERE medal = 'Silver'
        """,
    )

    count_gold = SQLExecuteQueryOperator(
        task_id="count_gold",
        conn_id="goit_mysql_db",
        sql="""
            INSERT INTO medal_counts (medal_type, `count`, created_at)
            SELECT 'Gold', COUNT(*), CURRENT_TIMESTAMP
            FROM athlete_event_results
            WHERE medal = 'Gold'
        """,
    )

    delay = PythonOperator(
        task_id="delay",
        python_callable=wait_five_seconds,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    check_recent_record = SqlSensor(
        task_id="check_recent_record",
        conn_id="goit_mysql_db",
        sql="""
            SELECT CASE
                WHEN MAX(created_at) >= CURRENT_TIMESTAMP - INTERVAL 30 SECOND
                THEN 1
                ELSE 0
            END
            FROM medal_counts
        """,
        poke_interval=5,
        timeout=40,
        mode="poke",
    )

    create_table >> choose_medal_task >> branch_medal

    branch_medal >> [
        count_bronze,
        count_silver,
        count_gold,
    ]

    [
        count_bronze,
        count_silver,
        count_gold,
    ] >> delay >> check_recent_record
