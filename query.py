import sqlite3
import pandas as pd
import time
from db import DB_PATH
from stream import query_region_from_stream
import folium
import datetime

def query_region_from_db(xmin, ymin, xmax, ymax, start_dt_str=None, end_dt_str=None):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    query = """
        SELECT longitude, latitude, timestamp, passenger_count, total_amount, tip_amount
        FROM stream_points
        WHERE longitude BETWEEN ? AND ? AND latitude BETWEEN ? AND ?
    """
    params = [xmin, xmax, ymin, ymax]

    if start_dt_str and end_dt_str:
        start_ts = int(datetime.datetime.strptime(start_dt_str, '%Y-%m-%d %H:%M:%S').timestamp())
        end_ts = int(datetime.datetime.strptime(end_dt_str, '%Y-%m-%d %H:%M:%S').timestamp())
        query += " AND timestamp BETWEEN ? AND ?"
        params += [start_ts, end_ts]

    c.execute(query, params)
    rows = c.fetchall()
    conn.close()

    print(f"\n📍 Zapytanie: ile punktów w obszarze ({xmin},{ymin})–({xmax},{ymax})")
    if start_dt_str and end_dt_str:
        print(f"   oraz czasie od {start_dt_str} do {end_dt_str}")
    print(f"➡️ Wynik: {len(rows)} punktów")

    if rows:
        avg_lon = sum(r[0] for r in rows) / len(rows)
        avg_lat = sum(r[1] for r in rows) / len(rows)
        avg_passengers = sum(r[3] for r in rows) / len(rows)
        avg_total = sum(r[4] for r in rows) / len(rows)
        avg_tip = sum(r[5] for r in rows) / len(rows)
        print(f"📌 Średnia lokalizacja: ({avg_lon:.5f}, {avg_lat:.5f})")
        print(f"🚖 Średnia liczba pasażerów: {avg_passengers:.2f}")
        print(f"💰 Średnia kwota całkowita: ${avg_total:.2f}, napiwek: ${avg_tip:.2f}")

    return rows

def map_query_and_centroid(xmin, ymin, xmax, ymax):
    mode = input("Wybierz tryb pracy:\n1 - zapytanie historyczne (z bazy danych)\n2 - zapytanie strumieniowe (z R-tree)\nTwój wybór (1/2): ").strip()

    if mode == '1':
        print("📂 Tryb: zapytanie historyczne (z bazy danych)")
        points = query_region_from_db(xmin, ymin, xmax, ymax)
    elif mode == '2':
        print("⚡ Tryb: zapytanie strumieniowe (z R-tree)")
        points = query_region_from_stream(xmin, ymin, xmax, ymax)
    else:
        print("❌ Niepoprawny wybór trybu.")
        return

    if not points:
        print("❌ Brak punktów do wyświetlenia na mapie.")
        return

    avg_lon = sum(p[0] for p in points) / len(points)
    avg_lat = sum(p[1] for p in points) / len(points)

    m = folium.Map(location=[avg_lat, avg_lon], zoom_start=13)
    for lon, lat, ts, p_count, total_amt, tip_amt in points:
        folium.CircleMarker(
            location=[lat, lon],
            radius=3,
            color='blue',
            fill=True,
            popup=f"Pasażerowie: {p_count}, $: {total_amt:.2f}, napiwek: {tip_amt:.2f}"
        ).add_to(m)

    bounds = [(ymin, xmin), (ymin, xmax), (ymax, xmax), (ymax, xmin), (ymin, xmin)]
    folium.PolyLine(bounds, color="green", weight=2.5, opacity=0.8).add_to(m)
    folium.Marker(location=[avg_lat, avg_lon], popup="Centroid", icon=folium.Icon(color='red')).add_to(m)

    filename = "mapa.html"
    m.save(filename)
    print(f"✅ Mapa zapisana jako {filename}")