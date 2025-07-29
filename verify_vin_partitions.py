import csv
import json
import os
from kafka_partition_finder import kafka_partition

NUM_PARTITIONS = 16

def load_txt(filepath):
    vins = []
    with open(filepath) as f:
        for line in f:
            if ':' not in line:
                continue
            vin_part, partition_part = line.strip().split(":")
            vin = vin_part.strip()
            expected_partition = int(partition_part.replace("Partition", "").strip())
            vins.append((vin, expected_partition))
    return vins

def load_csv(filepath):
    vins = []
    with open(filepath, newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            vin = row["vin"]
            expected_partition = int(row["partition"])
            vins.append((vin, expected_partition))
    return vins

def load_json(filepath):
    vins = []
    with open(filepath) as f:
        data = json.load(f)
        for entry in data:
            vins.append((entry["vin"], int(entry["partition"])))
    return vins

def verify(filepath):
    ext = os.path.splitext(filepath)[1]
    if ext == ".txt":
        vins = load_txt(filepath)
    elif ext == ".csv":
        vins = load_csv(filepath)
    elif ext == ".json":
        vins = load_json(filepath)
    else:
        raise ValueError("Unsupported file format. Use .txt, .csv, or .json")

    errors = []
    for vin, expected_partition in vins:
        actual_partition = kafka_partition(vin, NUM_PARTITIONS)
        if actual_partition != expected_partition:
            errors.append((vin, expected_partition, actual_partition))

    if not errors:
        print("All VINs correctly map to expected partitions.")
    else:
        print(f"Found {len(errors)} mismatches:")
        for vin, expected, actual in errors:
            print(f"  VIN: {vin} | Expected: {expected} | Actual: {actual}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python verify_vin_partitions.py <vin_file.txt|csv|json>")
        exit(1)

    verify(sys.argv[1])
