import csv
import os
import time

import mysql.connector

CSV_PATH = "/data/athlete_event_results.csv"

config = {
    "host": "mysql",
    "port": 3306,
    "user": "goit",
    "password": "goitpass",
    "database": "olympic_dataset",
}

while True:
    try:
        conn = mysql.connector.connect(**config)
        break
    except mysql.connector.Error as exc:
        print(f"MySQL ще не готовий: {exc}")
        time.sleep(3)

cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS athlete_event_results (
    edition VARCHAR(100),
    edition_id INT,
    country_noc VARCHAR(10),
    sport VARCHAR(100),
    event VARCHAR(200),
    result_id BIGINT,
    athlete VARCHAR(200),
    athlete_id BIGINT,
    pos VARCHAR(50),
    medal VARCHAR(20),
    isTeamSport BOOLEAN
)
""")

cursor.execute("SELECT COUNT(*) FROM athlete_event_results")
existing = cursor.fetchone()[0]

if existing == 0:
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        batch = []

        for row in reader:
            batch.append((
                row["edition"],
                int(row["edition_id"]),
                row["country_noc"],
                row["sport"],
                row["event"],
                int(row["result_id"]),
                row["athlete"],
                int(row["athlete_id"]),
                row["pos"] or None,
                row["medal"] or None,
                row["isTeamSport"].strip().lower() == "true",
            ))

            if len(batch) >= 1000:
                cursor.executemany("""
                    INSERT INTO athlete_event_results
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch)
                conn.commit()
                batch.clear()

        if batch:
            cursor.executemany("""
                INSERT INTO athlete_event_results
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            conn.commit()

cursor.execute("SELECT COUNT(*) FROM athlete_event_results")
print("Імпорт завершено. Кількість записів:", cursor.fetchone()[0])

cursor.close()
conn.close()
