import tqdm, requests, os

url_download = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}-{:02}.parquet"
os.mkdir("taxi-data")

for year in tqdm.tqdm(range(2019,2025), desc="Year", position=0):
    for month in tqdm.tqdm(range(1, 13), desc="Month", position=1):
        download = url_download.format(year, month)
        response = requests.get(download)
        with open(f'taxi-data/{year}-{month:02}.parquet', 'wb') as f:
            f.write(response.content)