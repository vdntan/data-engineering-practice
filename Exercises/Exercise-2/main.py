import requests
import pandas as pd
from bs4 import BeautifulSoup
from pathlib import Path
BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
TARGET_TIMESTAMP = "2024-01-19 10:27"  # Dấu thời gian cần tìm
DOWNLOAD_DIR = Path("downloads")
DOWNLOAD_DIR.mkdir(exist_ok=True)

def fetch_html(url):
    """Lấy nội dung HTML từ URL"""
    response = requests.get(url)
    response.raise_for_status()
    return response.text

def find_filename_by_timestamp(html, timestamp):
    """Tìm file tương ứng với mốc thời gian"""
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.find_all("tr")

    for row in rows:
        columns = row.find_all("td")
        if len(columns) >= 2:
            date_str = columns[1].text.strip()
            if date_str == timestamp:
                filename = columns[0].text.strip()
                return filename
    raise ValueError(f"❌ Không tìm thấy file với thời gian {timestamp}")

def download_file(filename):
    """Tải file CSV"""
    file_url = BASE_URL + filename
    dest_path = DOWNLOAD_DIR / filename
    print(f"⬇️  Downloading {file_url}")
    response = requests.get(file_url)
    response.raise_for_status()
    with open(dest_path, "wb") as f:
        f.write(response.content)
    print(f"✅ File saved to: {dest_path}")
    return dest_path

def analyze_temperature(file_path):
    """Phân tích dữ liệu nhiệt độ"""
    df = pd.read_csv(file_path)

    if 'HourlyDryBulbTemperature' not in df.columns:
        raise ValueError("❌ Cột 'HourlyDryBulbTemperature' không tồn tại trong file.")

    df_clean = df.dropna(subset=['HourlyDryBulbTemperature'])
    max_temp = df_clean['HourlyDryBulbTemperature'].max()
    max_rows = df_clean[df_clean['HourlyDryBulbTemperature'] == max_temp]

    print(f"\n🌡️  Max HourlyDryBulbTemperature: {max_temp}")
    print("📊 Rows with highest temperature:")
    print(max_rows)


def main():
    # your code here
    print("🔍 Fetching HTML...")
    html = fetch_html(BASE_URL)

    print("🔎 Finding file for timestamp...")
    filename = find_filename_by_timestamp(html, TARGET_TIMESTAMP)

    print("📥 Downloading data...")
    file_path = download_file(filename)

    print("📊 Analyzing temperature data...")
    analyze_temperature(file_path)

if __name__ == "__main__":
    main()
