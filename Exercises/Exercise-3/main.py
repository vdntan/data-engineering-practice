import requests, gzip, io

BASE = "https://data.commoncrawl.org/"
INDEX_PATH = "crawl-data/CC-MAIN-2022-05/wet.paths.gz"

def fetch_first_wet_path():
    url = BASE + INDEX_PATH
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    # decompress in-memory
    buf = io.BytesIO(resp.content)
    with gzip.GzipFile(fileobj=buf) as f:
        # read only the first line
        return f.readline().decode("utf-8").strip()

def stream_wet_segment(first_path):
    url = BASE + first_path
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    # decompress the gz stream as we iterate
    with gzip.GzipFile(fileobj=resp.raw) as gz:
        count = 0
        for raw in gz:
            print(raw.decode("utf-8").rstrip())
            count += 1
            if count >= 10:
                break

def main():
    print("Downloading index…")
    first_path = fetch_first_wet_path()
    print("First WET segment:", first_path, "\nStreaming now:\n")
    stream_wet_segment(first_path)

if __name__ == "__main__":
    main()
