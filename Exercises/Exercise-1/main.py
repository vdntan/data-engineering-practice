import os
import requests
from zipfile import ZipFile

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

DOWNLOAD_DIR = "downloads"


def download_and_extract(url):
    filename = url.split("/")[-1]
    zip_path = os.path.join(DOWNLOAD_DIR, filename)

    try:
        print(f"Downloading {filename}...")
        response = requests.get(url)
        response.raise_for_status()
        with open(zip_path, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {filename}")

        with ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
        print(f"Extracted: {filename}")

        os.remove(zip_path)
        print(f"Deleted zip: {filename}\n")

    except Exception as e:
        print(f"❌ Failed to process {filename}: {e}\n")

def main():
    # your code here
    if not os.path.exists(DOWNLOAD_DIR):
        os.makedirs(DOWNLOAD_DIR)
        print(f"Created directory: {DOWNLOAD_DIR}\n")

    for url in download_uris:
        download_and_extract(url)


if __name__ == "__main__":
    main()
