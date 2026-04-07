import sqlite3
from db import init_db, clear_db, load_data_to_db
from stream import stream_from_db
from olap import create_olap_table, build_olap_cube, query_olap_cube
from query import map_query_and_centroid

if __name__ == "__main__":
    init_db()
    clear_db()

    if load_data_to_db("nyc_taxi_data_2014.csv", "2014-01-01 00:00:00", "2014-02-28 18:00:00"):
        stream_from_db()
        create_olap_table()
        build_olap_cube()
        map_query_and_centroid(-74.25909, 40.4774, -73.70018, 40.9176)
        query_olap_cube(day="2014-01-09", hour=10)
    else:
        print("🚫 Nie załadowano danych.")