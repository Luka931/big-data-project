import requests
import zipfile
import os

def unzip_and_delete(file_path):
    
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(file_path.split(".")[0])
    
    os.remove(file_path)

# Download all the data
print("Downloading Weather data!")
weather_api = "https://archive-api.open-meteo.com/v1/archive?latitude=40.73061&longitude=-73.935242&start_date=2020-01-01&end_date=2025-01-01&hourly=temperature_2m,rain,snowfall,snow_depth,wind_speed_10m,wind_gusts_10m&timezone=America%2FNew_York&format=csv"
reposne = requests.get(weather_api)
with open("weather-data.csv", 'wb') as f:
    f.write(reposne.content)

print("Downloading Taxi zone data!")
zone_shapes_api = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
reposne = requests.get(zone_shapes_api)
with open("taxi-zones.zip", 'wb') as f:
    f.write(reposne.content)
unzip_and_delete("taxi-zones.zip")

print("Downloading School lications data!")
school_locations_api = "https://data.cityofnewyork.us/download/jfju-ynrr/application%2Fx-zip-compressed"
reposne = requests.get(school_locations_api)
with open("school-locations.zip", 'wb') as f:
    f.write(reposne.content)
unzip_and_delete("school-locations.zip")

print("Downloading University locations data!")
universities_api = "https://data.cityofnewyork.us/api/views/8pnn-kkif/rows.csv?date=20250414&accessType=DOWNLOAD"
reposne = requests.get(universities_api)
with open("universities.csv", 'wb') as f:
    f.write(reposne.content)

print("Downloading Points of interest data!")
points_of_interest_api = "https://data.cityofnewyork.us/api/views/t95h-5fsr/rows.csv?date=20250414&accessType=DOWNLOAD"
reposne = requests.get(points_of_interest_api)
with open("points-of-interest.csv", 'wb') as f:
    f.write(reposne.content)

# print("Downloading Event locations data!")
# event_locations_api = "https://data.cityofnewyork.us/resource/cpcm-i88g.csv?$limit=100000"
# reposne = requests.get(event_locations_api)
# with open("event-locations.csv", 'wb') as f:
#     f.write(reposne.content)

# print("Downloading Events data!")
# events_api = "https://data.cityofnewyork.us/resource/fudw-fgrp.csv?$limit=100000"
# reposne = requests.get(events_api)
# with open("events.csv", 'wb') as f:
#     f.write(reposne.content)
