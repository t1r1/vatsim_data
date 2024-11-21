import requests
import json
import sqlite3
from datetime import datetime

def fetch_data():
  r = requests.get("https://data.vatsim.net/v3/vatsim-data.json")

  if r.status_code == 200:
   return r.json()
  else:
    print(f"Failed to fetch data. Status code: {r.status_code}")


def store_data(data):
  conn = sqlite3.connect("vatsim_data.db")
  cursor = conn.cursor()

  cursor.execute("""
      CREATE TABLE IF NOT EXISTS daily_data (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          fetch_date TEXT UNIQUE,
          data TEXT
      )
    """)

  fetch_date = datetime.now().strftime("%Y-%m-%d")
  cursor.execute("INSERT INTO daily_data (fetch_date, data) VALUES (?, ?)", (fetch_date, json.dumps(data)))

  conn.commit()
  conn.close()
  print(f"Data stored in database for {fetch_date}")

data = fetch_data()
store_data(data)
