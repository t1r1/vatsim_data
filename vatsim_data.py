import requests
import json
import os
import time
from datetime import datetime, UTC

def fetch_data():
    r = requests.get("https://data.vatsim.net/v3/vatsim-data.json")
    if r.status_code == 200:
        return r.json()
    else:
        print(f"Failed to fetch data. Status code: {r.status_code}")
        return None

def create_directory(directory_name): # References: [1]
    try:
        os.makedirs(directory_name, exist_ok=True)
        print(f"Directory '{directory_name}' is ready.")
    except PermissionError:
        print(f"Permission denied: Unable to create '{directory_name}'.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None

    return os.path.abspath(directory_name)

def retrieve(prev_timestamp):
    data = fetch_data()
    if data is None:
        return

    created_at = datetime.now(UTC).date().isoformat() # References: [4]
    dir_name = create_directory(created_at)

    if dir_name is None:
        print("Directory creation failed. Exiting.")
        return

    # using the "update" timestamp from the data for the file name
    timestamp = int(data["general"]["update"])
    if prev_timestamp is not None and prev_timestamp >= timestamp:
        print("data is old or hasn't changed")
        return prev_timestamp

    file_name_full = f'{timestamp}.json'  # References: [2]
    file_path = os.path.join(dir_name, file_name_full)

    with open(file_path, "w") as f:
        json.dump(data, f)

    print(f"Data written to: {file_path}")
    return timestamp


def main():
    current_timestamp = None
    while True:
        print('retrieve')
        new_timestamp = retrieve(current_timestamp)
        if current_timestamp is not None and new_timestamp <= current_timestamp:
            time.sleep(3)
        else:
            print('sleeping')
            time.sleep(15)  # References: [3]
        current_timestamp = new_timestamp


if __name__ == "__main__":
    main()
