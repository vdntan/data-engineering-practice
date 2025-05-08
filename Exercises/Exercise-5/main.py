import glob
import json
import csv
import os

def flatten_json(data):
    """Flatten nested JSON objects."""
    flat_data = {}
    for key, value in data.items():
        if isinstance(value, dict):
            for sub_key, sub_value in value.items():
                flat_data[f"{key}_{sub_key}"] = sub_value
        elif isinstance(value, list):
            flat_data[key] = ",".join(map(str, value))
        else:
            flat_data[key] = value
    return flat_data

def json_to_csv(json_path, output_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Nếu là danh sách JSON objects, xử lý từng phần tử
    if isinstance(data, list):
        flat_data_list = [flatten_json(item) for item in data]
    else:
        flat_data_list = [flatten_json(data)]

    # Ghi CSV
    with open(output_path, 'w', newline='', encoding='utf-8') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=flat_data_list[0].keys())
        writer.writeheader()
        for item in flat_data_list:
            writer.writerow(item)

def main():
    # your code here
    json_files = glob.glob("data/**/*.json", recursive=True)
    print(f"Found {len(json_files)} JSON files.")

    for json_file in json_files:
        csv_file = json_file.replace(".json", ".csv")
        print(f"Converting {json_file} -> {csv_file}")
        
        os.makedirs(os.path.dirname(csv_file), exist_ok=True)
        json_to_csv(json_file, csv_file)


if __name__ == "__main__":
    main()
