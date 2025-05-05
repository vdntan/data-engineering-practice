import requests
from bs4 import BeautifulSoup
import pandas as pd
import os

BASE_URL = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
DOWNLOAD_DIR = "downloads"
TARGET_DATETIME = "2024-01-19 10:27"

def main():
    # Bước 1: Lấy nội dung HTML
    response = requests.get(BASE_URL)
    if response.status_code != 200:
        print("Failed to retrieve webpage.")
        return
    
    # Bước 2: Phân tích HTML để tìm file theo thời gian chỉnh sửa
    soup = BeautifulSoup(response.text, "html.parser")
    rows = soup.find_all("tr")

    target_filename = None
    for row in rows:
        columns = row.find_all("td")
        if len(columns) >= 2:
            date_str = columns[1].text.strip()
            if date_str == TARGET_DATETIME:
                link = row.find("a")
                if link and "href" in link.attrs:
                    target_filename = link["href"]
                    break

    if not target_filename:
        print("Không tìm thấy file phù hợp.")
        return

    # Bước 3: Tải file CSV về
    file_url = BASE_URL + target_filename
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    local_path = os.path.join(DOWNLOAD_DIR, target_filename)

    print(f"Tải file từ: {file_url}")
    with requests.get(file_url, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    # Bước 4: Đọc file CSV bằng pandas
    try:
        df = pd.read_csv(local_path)
    except Exception as e:
        print(f"Lỗi khi đọc CSV: {e}")
        return

    # Bước 5: Tìm nhiệt độ cao nhất
    if "HourlyDryBulbTemperature" not in df.columns:
        print("Không tìm thấy cột HourlyDryBulbTemperature.")
        return

    max_temp = df["HourlyDryBulbTemperature"].max()
    hottest_rows = df[df["HourlyDryBulbTemperature"] == max_temp]

    print("\n📌 Bản ghi có nhiệt độ khô cao nhất:")
    print(hottest_rows)

if __name__ == "__main__":
    main()
