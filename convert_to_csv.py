import os
import compress.api as compress
import json
import csv

def readdir(name):
  filenames = os.listdir(name)
  filenames.sort(key=sort_key)
  print(filenames)
  for filename in filenames:
    full_path = os.path.join(name, filename)
    with open(full_path, "rb") as file:
      rawdata = file.read()
      json_data = compress.decompress(data=rawdata, algo=compress.Algorithm.lzma)
      data = json.loads(json_data)
      yield data


def sort_key(filename):
  return int(filename.split(".")[0])

def prepare_pilots_csv():
  with open('pilots.csv', 'w', newline='') as csvfile:
    fieldnames = ['update', 'cid', 'name', 'callsign', 'latitude', 'aircraft_short', 'departure', 'arrival', 'enroute_time']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for item in readdir("vatsim_data_debug_chunk"):
      for pilot in item["pilots"]:
        row = {'update': item["general"]["update"], 'cid': pilot["cid"],'name': pilot["name"], 'callsign': pilot["callsign"]}

        # handling missing or inconstent data
        row['latitude'] = (pilot["latitude"] or {})
        row['departure'] = (pilot["flight_plan"] or {}).get("departure")
        row["aircraft_short"] = (pilot["flight_plan"] or {}).get("aircraft_short")
        row["arrival"] = (pilot["flight_plan"] or {}).get("arrival")
        row["enroute_time"] = (pilot["flight_plan"] or {}).get("enroute_time")
        writer.writerow(row)


def prepare_controllers_csv():
  with open('controllers.csv', 'w', newline='') as csvfile:
    fieldnames = ['update', 'cid', 'name', 'callsign']
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
    writer.writeheader()

    for item in readdir("vatsim_data_debug_chunk"):
      for controller in item["controllers"]:
        row = {'update': item["general"]["update"], 'cid': controller["cid"],'name': controller["name"], 'callsign': controller["callsign"]}
        writer.writerow(row)

if __name__ == "__main__":
  prepare_pilots_csv()
  prepare_controllers_csv()
