import sqlite3
from db import DB_PATH

def create_olap_table():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS olap_cube (
            day TEXT,
            hour INTEGER,
            lat_bin INTEGER,
            lon_bin INTEGER,
            passenger_group TEXT,
            count INTEGER,
            total_amount_sum REAL,
            tip_amount_avg REAL
        )
    ''')
    conn.commit()
    conn.close()

def build_olap_cube():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Wyczyść starą zawartość
    c.execute("DELETE FROM olap_cube")

    # Zbuduj kostkę
    query = """
        INSERT INTO olap_cube
        SELECT
            strftime('%Y-%m-%d', datetime(timestamp, 'unixepoch')) AS day,
            CAST(strftime('%H', datetime(timestamp, 'unixepoch')) AS INTEGER) AS hour,
            CAST(latitude * 100 AS INT) AS lat_bin,
            CAST(longitude * 100 AS INT) AS lon_bin,
            CASE
                WHEN passenger_count = 1 THEN '1'
                WHEN passenger_count BETWEEN 2 AND 3 THEN '2-3'
                ELSE '4+'
            END AS passenger_group,
            COUNT(*) AS count,
            SUM(total_amount) AS total_amount_sum,
            AVG(tip_amount) AS tip_amount_avg
        FROM stream_points
        GROUP BY day, hour, lat_bin, lon_bin, passenger_group
    """
    c.execute(query)
    conn.commit()
    conn.close()

def query_olap_cube(day=None, hour=None, lat_bin=None, lon_bin=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    query = "SELECT * FROM olap_cube WHERE 1=1"
    params = []

    if day:
        query += " AND day = ?"
        params.append(day)
    if hour is not None:
        query += " AND hour = ?"
        params.append(hour)
    if lat_bin is not None:
        query += " AND lat_bin = ?"
        params.append(lat_bin)
    if lon_bin is not None:
        query += " AND lon_bin = ?"
        params.append(lon_bin)

    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    print("\n📊 Wyniki zapytania OLAP:")
    for row in rows:
        print(f"{row}")

    if not rows:
        print("❌ Brak wyników dla podanych parametrów.")