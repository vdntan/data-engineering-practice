import os
import glob
import json
import csv

def find_json_files(base_dir):
    # Tìm tất cả các file .json trong thư mục con của data
    return glob.glob(os.path.join(base_dir, '**', '*.json'), recursive=True)

def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if isinstance(x, dict):
            for a in x:
                flatten(x[a], f'{name}{a}_')
        elif isinstance(x, list):
            for i, a in enumerate(x):
                flatten(a, f'{name}{i}_')
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def json_to_csv(json_file):
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Đảm bảo mọi json đều là dict (hoặc list các dict)
    if isinstance(data, list):
        flat_data = [flatten_json(entry) for entry in data]
    else:
        flat_data = [flatten_json(data)]

    # Tạo tên file .csv tương ứng
    csv_file = os.path.splitext(json_file)[0] + '.csv'

    with open(csv_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=flat_data[0].keys())
        writer.writeheader()
        writer.writerows(flat_data)

    print(f"✅ Converted: {json_file} → {csv_file}")

def main():
    base_dir = 'data'
    json_files = find_json_files(base_dir)

    if not json_files:
        print("❌ No JSON files found!")
        return

    for json_file in json_files:
        try:
            json_to_csv(json_file)
        except Exception as e:
            print(f"⚠️ Failed to process {json_file}: {e}")

if __name__ == '__main__':
    main()
