import requests
import os
import zipfile
from pathlib import Path
download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
]
DOWNLOAD_DIR = Path("downloads")

def create_download_dir():
    """Create the downloads directory if it doesn't exist."""
    if not DOWNLOAD_DIR.exists():
        print("Creating 'downloads' directory...")
        DOWNLOAD_DIR.mkdir(parents=True)
    else:
        print("'downloads' directory already exists.")

def get_filename_from_url(url):
    """Extract filename from URL."""
    return url.split("/")[-1]

def download_file(url, dest_path):
    """Download a single file from a URL."""
    try:
        response = requests.get(url, stream=True, timeout=10)
        response.raise_for_status()
        with open(dest_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        print(f"Downloaded: {dest_path.name}")
        return True
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        return False

def unzip_file(zip_path):
    """Unzip a .zip file and delete the original .zip."""
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(DOWNLOAD_DIR)
        print(f"Extracted: {zip_path.name}")
        zip_path.unlink()  # Delete the zip file
    except zipfile.BadZipFile as e:
        print(f"Bad zip file {zip_path.name}: {e}")



def main():
    # your code here
     create_download_dir()
     for url in download_uris:
        filename = get_filename_from_url(url)
        zip_path = DOWNLOAD_DIR / filename

        if download_file(url, zip_path):
            unzip_file(zip_path)


if __name__ == "__main__":
    main()
