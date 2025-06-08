import tqdm, requests, os


def make_directory(dir_name):
    if not os.path.isdir(dir_name):
        os.mkdir(dir_name)


def download_data_set(api_data):
    url_download = api_data.get("url")
    end = api_data.get("end")
    start = api_data.get("start")
    save_location = (
        "trip_record_data/" + api_data.get("save_folder") + "/{}-{:02}.parquet"
    )

    make_directory(api_data.get("save_folder"))

    for year in tqdm.tqdm(range(start, end), desc="Year", position=0):
        for month in tqdm.tqdm(range(1, 13), desc="Month", position=1):
            download = url_download.format(year, month)
            response = requests.get(download)
            with open(save_location.format(year, month), "wb") as f:
                f.write(response.content)


YELLOW_TAXI_API = {
    "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{:02}.parquet",
    "save_folder": "yellow-taxi",
    "start": 2012,
    "end": 2025,
}

GREEN_TAXI_API = {
    "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{}-{:02}.parquet",
    "save_folder": "green_taxi",
    "start": 2014,
    "end": 2025,
}

FOR_HIRE_API = {
    "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhv_tripdata_{}-{:02}.parquet",
    "save_folder": "for_hire",
    "start": 2015,
    "end": 2025,
}

HIGH_VOLUME_API = {
    "url": "https://d37ci6vzurychx.cloudfront.net/trip-data/fhvhv_tripdata_{}-{:02}.parquet",
    "save_folder": "high_volume",
    "start": 2019,
    "end": 2025,
}


API_DATA = {
    "yellow-taxi": YELLOW_TAXI_API,
    "green-taxi": GREEN_TAXI_API,
    "for-hire": FOR_HIRE_API,
    "high-volume": HIGH_VOLUME_API,
}


if __name__ == "__main__":
    user_prompt = f"""Input one of the following values to download specific data {"\n".join(API_DATA.keys())}, input anything else to exit."""

    while True:
        recieved_input = input(user_prompt)

        if recieved_input in API_DATA:
            download_data_set(API_DATA[recieved_input])
        else:
            break
