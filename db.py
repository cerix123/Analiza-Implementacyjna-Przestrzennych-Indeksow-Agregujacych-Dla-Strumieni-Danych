import sqlite3
import pandas as pd

DB_PATH = "stream_data.db"
MAX_POINTS = 10000

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS stream_points (
            id INTEGER PRIMARY KEY,
            timestamp INTEGER,
            longitude REAL,
            latitude REAL,
            passenger_count INTEGER,
            total_amount REAL,
            tip_amount REAL
        )
    ''')
    conn.commit()
    conn.close()

def clear_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("DELETE FROM stream_points")
    conn.commit()
    conn.close()

def insert_points_bulk(df):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()

    # Filtrujemy rekordy, które mają latitude i longitude różne od 0
    filtered_df = df[(df['pickup_latitude'] != 0) & (df['pickup_longitude'] != 0)]

    data = [
        (i,
         int(row['timestamp']),
         float(row['pickup_longitude']),
         float(row['pickup_latitude']),
         int(row['passenger_count']),
         float(row['total_amount']),
         float(row['tip_amount']))
        for i, row in filtered_df.iterrows()
    ]
    c.executemany('''
        INSERT INTO stream_points (
            id, timestamp, longitude, latitude, passenger_count, total_amount, tip_amount
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
    ''', data)
    conn.commit()
    conn.close()

def load_data_to_db(file_path, start_dt_str=None, end_dt_str=None):
    MIN_DATE_STR = "2014-01-01 00:00:00"
    MAX_DATE_STR = "2014-02-28 23:59:59"
    min_date = pd.to_datetime(MIN_DATE_STR)
    max_date = pd.to_datetime(MAX_DATE_STR)

    if start_dt_str and end_dt_str:
        try:
            start_dt = pd.to_datetime(start_dt_str)
            end_dt = pd.to_datetime(end_dt_str)
        except Exception:
            print("❌ Niepoprawny format daty. Użyj formatu 'YYYY-MM-DD HH:MM:SS'.")
            return False

        if start_dt < min_date or end_dt > max_date:
            print(f"❌ Podany zakres dat jest poza dopuszczalnym zakresem {MIN_DATE_STR} - {MAX_DATE_STR}.")
            return False
        if start_dt > end_dt:
            print("❌ Data początkowa musi być wcześniejsza niż data końcowa.")
            return False
    else:
        start_dt = min_date
        end_dt = max_date

    df = pd.read_csv(file_path, usecols=[
        'pickup_datetime', 'pickup_longitude', 'pickup_latitude',
        'passenger_count', 'total_amount', 'tip_amount'
    ])
    df = df.dropna()
    df = df[df['pickup_longitude'].between(-180, 180)]
    df = df[df['pickup_latitude'].between(-90, 90)]

    df['timestamp'] = pd.to_datetime(df['pickup_datetime'])
    df = df[(df['timestamp'] >= start_dt) & (df['timestamp'] <= end_dt)]
    df['timestamp'] = df['timestamp'].astype(int) // 10**9
    df = df.head(MAX_POINTS)

    print(f"✅ Załadowano {len(df)} rekordów do bazy.")
    insert_points_bulk(df)
    return True