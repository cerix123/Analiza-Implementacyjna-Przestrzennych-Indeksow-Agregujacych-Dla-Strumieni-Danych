from collections import deque
from rtree import index
from shapely.geometry import box
import sqlite3
import time

TIME_WINDOW = 10000
DB_PATH = "stream_data.db"
data_stream = deque()
rtree_idx = index.Index()

def stream_from_db():
    global point_id
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, timestamp, longitude, latitude, passenger_count, total_amount, tip_amount FROM stream_points ORDER BY timestamp ASC")
    rows = c.fetchall()
    conn.close()

    for pid, ts, x, y, p_count, total_amt, tip_amt in rows:
        data_stream.append((ts, pid, (x, y), p_count, total_amt, tip_amt))
        rtree_idx.insert(pid, (x, y, x, y), obj={'timestamp': ts})
        remove_old_points(ts)
        print(f"[{time.strftime('%X')}] Strumień: ({x:.4f}, {y:.4f}), pasażerów: {p_count}, $: {total_amt}, napiwek: {tip_amt}")
        point_id = pid + 1

def remove_old_points(current_time):
    while data_stream and current_time - data_stream[0][0] > TIME_WINDOW:
        old_ts, old_id, (x, y), *_ = data_stream.popleft()
        try:
            rtree_idx.delete(old_id, (x, y, x, y))
        except:
            pass

def query_region_from_stream(xmin, ymin, xmax, ymax):
    box_query = box(xmin, ymin, xmax, ymax)
    results = []

    for hit in list(rtree_idx.intersection((xmin, ymin, xmax, ymax), objects=True)):
        pid = hit.id
        ts = hit.object['timestamp']

        for d_ts, d_pid, (lon, lat), p_count, total_amt, tip_amt in data_stream:
            if d_pid == pid:
                if box_query.contains(box(lon, lat, lon, lat)):
                    results.append((lon, lat, ts, p_count, total_amt, tip_amt))
                break

    print(f"\n📍 Zapytanie strumieniowe: ({xmin},{ymin})–({xmax},{ymax})")
    print(f"➡️ Wynik: {len(results)} punktów")

    if results:
        avg_lon = sum(r[0] for r in results) / len(results)
        avg_lat = sum(r[1] for r in results) / len(results)
        avg_passengers = sum(r[3] for r in results) / len(results)
        avg_total = sum(r[4] for r in results) / len(results)
        avg_tip = sum(r[5] for r in results) / len(results)
        print(f"📌 Średnia lokalizacja: ({avg_lon:.5f}, {avg_lat:.5f})")
        print(f"🚖 Średnia liczba pasażerów: {avg_passengers:.2f}")
        print(f"💰 Średnia kwota całkowita: ${avg_total:.2f}, napiwek: ${avg_tip:.2f}")

    return results