import requests, os, zipfile, tqdm


def unzip_and_delete(file_path):
    with zipfile.ZipFile(file_path, "r") as zip_ref:
        zip_ref.extractall(file_path.split(".")[0])

    os.remove(file_path)


def download_from_api(api_data):
    reposne = requests.get(api_data["url"])
    save_file_name = api_data["save_file_name"]
    with open(save_file_name, "wb") as f:
        f.write(reposne.content)

    if "unzip_and_delete" in api_data and api_data["unzip_and_delete"]:
        unzip_and_delete(save_file_name)


WEATHER_API = {
    "url": "https://archive-api.open-meteo.com/v1/archive?latitude=40.73061&longitude=-73.935242&start_date=2012-01-01&end_date=2025-01-01&hourly=temperature_2m,rain,snowfall,snow_depth,wind_speed_10m,wind_gusts_10m&timezone=America%2FNew_York&format=csv",
    "save_file_name": "weather.csv",
}

ZONE_SHAPE_API = {
    "url": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip",
    "save_file_name": "zones_shape.zip",
    "unzip_and_delete": True,
}

SCHOOL_LOCATIONS_API = {
    "url": "https://data.cityofnewyork.us/download/jfju-ynrr/application%2Fx-zip-compressed",
    "save_file_name": "school_locations.zip",
    "unzip_and_delete": True,
}

UNIVERSITY_LOCATIONS_API = {
    "url": "https://data.cityofnewyork.us/api/views/8pnn-kkif/rows.csv?date=20250414&accessType=DOWNLOAD",
    "save_file_name": "university_locations.csv",
}

POINTS_OF_INTEREST_API = {
    "url": "https://data.cityofnewyork.us/api/views/t95h-5fsr/rows.csv?date=20250414&accessType=DOWNLOAD",
    "save_file_name": "points_of_interest.csv",
}

API_DATA = {
    "weather": WEATHER_API,
    "zone": ZONE_SHAPE_API,
    "schools": SCHOOL_LOCATIONS_API,
    "universities": UNIVERSITY_LOCATIONS_API,
    "poi": POINTS_OF_INTEREST_API,
}

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

if __name__ == "__main__":

    user_prompt = f"""Input one of the following values to download specific data {"\n".join(API_DATA.keys())} or input 'all' to download everything, input anything else to exit."""

    while True:
        recieved_input = input(user_prompt)

        if recieved_input in API_DATA:
            download_from_api(API_DATA[recieved_input])
        elif recieved_input.lower() == "all":
            for value in tqdm.tqdm(API_DATA.values()):
                download_from_api(value)
            break
        else:
            break
