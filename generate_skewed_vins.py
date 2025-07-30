import random
import string
import json
import csv
import argparse
from kafka_partition_finder import kafka_partition

# Total Kafka partitions
NUM_PARTITIONS = 16

# Skewed distribution: VINs to generate per partition
VIN_COUNTS = {
    0: 1000,
    1: 500,
    2: 100,
    3: 100,
    4: 300,
    5: 50,
    6: 50,
    7: 200,
    8: 0,
    9: 10,
    10: 5,
    11: 5,
    12: 30,
    13: 20,
    14: 15,
    15: 5,
}

VIN_PREFIX = "TEST"
VIN_LENGTH = 17
EXPORT_FORMAT = "csv"  # Options: txt, csv, json
EXPORT_FILENAME = f"vin_keys_skewed.{EXPORT_FORMAT}"


def generate_random_vin_with_prefix(prefix):
    vin_chars = string.ascii_uppercase + string.digits
    vin_chars = vin_chars.replace('I', '').replace('O', '').replace('Q', '')
    remaining_length = VIN_LENGTH - len(prefix)
    return prefix + ''.join(random.choices(vin_chars, k=remaining_length))


def generate_vins_for_partitions(vin_counts):
    partition_map = {p: [] for p in vin_counts.keys()}
    seen = set()
    attempts = 0
    max_attempts = sum(vin_counts.values()) * 10

    while any(len(vins) < count for p, count in vin_counts.items() for vins in [partition_map[p]]):
        vin = generate_random_vin_with_prefix(VIN_PREFIX)
        if vin in seen:
            continue
        partition = kafka_partition(vin, NUM_PARTITIONS)
        if partition in vin_counts and len(partition_map[partition]) < vin_counts[partition]:
            partition_map[partition].append({"vin": vin, "partition": partition})
            seen.add(vin)
        attempts += 1
        if attempts > max_attempts:
            raise RuntimeError("Too many attempts. Adjust skewed counts or randomness.")

    return partition_map


def flatten_vins(partition_map):
    vins = []
    for entries in partition_map.values():
        vins.extend(entries)
    return vins


def export_vins(vins, format, filename):
    if format == "txt":
        with open(filename, "w") as f:
            for entry in vins:
                f.write(f"{entry['vin']} : Partition {entry['partition']}\n")
    elif format == "csv":
        with open(filename, "w", newline='') as f:
            writer = csv.DictWriter(f, fieldnames=["vin", "partition"])
            writer.writeheader()
            writer.writerows(vins)
    elif format == "json":
        with open(filename, "w") as f:
            json.dump(vins, f, indent=2)
    else:
        raise ValueError("Unsupported export format")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate evenly distributed VINs.")
    parser.add_argument("--format", choices=["txt", "csv", "json"], default=EXPORT_FORMAT)
    parser.add_argument("--filename", default=EXPORT_FILENAME)
    try:
        vin_map = generate_vins_for_partitions(VIN_COUNTS)
        all_vins = flatten_vins(vin_map)

        print(f"Generated {len(all_vins)} VINs with skewed distribution across {NUM_PARTITIONS} partitions.")
        export_vins(all_vins, EXPORT_FORMAT, EXPORT_FILENAME)
        print(f"Saved to: {EXPORT_FILENAME}")
    except Exception as e:
        print(f"Error: {e}")
