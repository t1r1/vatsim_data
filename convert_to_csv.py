import os
import compress.api as compress
import json
import csv

def readdir(name):
  filenames = os.listdir(name)
  filenames.sort(key=sort_key)
  for filename in filenames:
    full_path = os.path.join(name, filename)
    with open(full_path, "rb") as file:
      rawdata = file.read()
      json_data = compress.decompress(data=rawdata, algo=compress.Algorithm.lzma)
      data = json.loads(json_data)
      yield data


def sort_key(filename):
  return int(filename.split(".")[0])

def prepare_csv():
  for dir_name in os.listdir("."):
    if dir_name.startswith("2024"):

      controllers = open("controllers.csv", "w", newline='')
      controllers_fieldnames = ['update', 'cid', 'name', 'callsign']
      controllers_writer = csv.DictWriter(controllers, fieldnames=controllers_fieldnames)

      pilots = open("pilots.csv", "w", newline='')
      pilots_fieldnames = ['update', 'cid', 'name', 'callsign', 'latitude', 'longitude', 'aircraft_short', 'departure', 'arrival', 'enroute_time']
      pilots_writer = csv.DictWriter(pilots, fieldnames=pilots_fieldnames)

      controllers_writer.writeheader()
      pilots_writer.writeheader()

      for item in readdir(dir_name):
        for controller in item["controllers"]:
          row = {'update': item["general"]["update"], 'cid': controller["cid"],'name': controller["name"], 'callsign': controller["callsign"]}
          controllers_writer.writerow(row)

        for pilot in item["pilots"]:
          row = {'update': item["general"]["update"], 'cid': pilot["cid"],'name': pilot["name"], 'callsign': pilot["callsign"]}

          # handling missing or inconstent data
          row['latitude'] = pilot["latitude"]
          row['longitude'] = pilot["longitude"]
          row['departure'] = (pilot["flight_plan"] or {}).get("departure")
          row["aircraft_short"] = (pilot["flight_plan"] or {}).get("aircraft_short")
          row["arrival"] = (pilot["flight_plan"] or {}).get("arrival")
          row["enroute_time"] = (pilot["flight_plan"] or {}).get("enroute_time")
          pilots_writer.writerow(row)


if __name__ == "__main__":
  prepare_csv()
