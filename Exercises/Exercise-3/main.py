import requests
import gzip
import io

BASE_URL = "https://data.commoncrawl.org/"
WET_PATHS_GZ = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

def download_gz_file(url):
    print(f"Downloading gzipped file: {url}")
    response = requests.get(url)
    response.raise_for_status()
    return response.content  # Trả về dữ liệu .gz dạng bytes

def extract_first_path(gz_bytes):
    print("Extracting first path from gzipped content...")
    with gzip.GzipFile(fileobj=io.BytesIO(gz_bytes)) as gz:
        first_line = gz.readline().decode('utf-8').strip()
        print(f"Found first WET file path: {first_line}")
        return first_line

def stream_wet_file_lines(wet_url):
    print(f"Streaming WET file from: {wet_url}")
    response = requests.get(wet_url, stream=True)
    response.raise_for_status()

    for line in response.iter_lines():
        try:
            yield line.decode('utf-8')
        except UnicodeDecodeError:
            # Bỏ qua dòng không thể giải mã
            continue
def main():
     gz_bytes = download_gz_file(BASE_URL + WET_PATHS_GZ)

    # Bước 2: Giải nén và lấy dòng đầu tiên
     first_path = extract_first_path(gz_bytes)

    # Bước 3: Tạo URL hoàn chỉnh và stream dòng
     full_url = BASE_URL + first_path
     for line in stream_wet_file_lines(full_url):
        print(line)


if __name__ == "__main__":
    main()
