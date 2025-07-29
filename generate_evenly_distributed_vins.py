import random
import string
import json
import csv
import argparse
from kafka_partition_finder import kafka_partition

NUM_PARTITIONS = 16
VINS_PER_PARTITION = 100
EXPORT_FORMAT = "txt"  # Options: "txt", "csv", "json"
EXPORT_FILENAME = f"vin_keys.{EXPORT_FORMAT}"
VIN_PREFIX = "TEST"
VIN_LENGTH = 17

def generate_random_vin_with_prefix(prefix):
    vin_chars = string.ascii_uppercase + string.digits
    vin_chars = vin_chars.replace('I', '').replace('O', '').replace('Q', '')
    remaining_length = VIN_LENGTH - len(prefix)
    return prefix + ''.join(random.choices(vin_chars, k=remaining_length))

def generate_vins_evenly_distributed(num_partitions, vins_per_partition):
    partition_map = {p: [] for p in range(num_partitions)}
    attempts = 0
    max_attempts = vins_per_partition * num_partitions * 10

    while any(len(vins) < vins_per_partition for vins in partition_map.values()):
        vin = generate_random_vin_with_prefix(VIN_PREFIX)
        partition = kafka_partition(vin, num_partitions)
        if len(partition_map[partition]) < vins_per_partition:
            partition_map[partition].append({"vin": vin, "partition": partition})
        attempts += 1
        if attempts > max_attempts:
            raise RuntimeError("Too many attempts. Try lowering vins_per_partition.")

    return partition_map

def flatten_vins(partition_map):
    vins = []
    for partition, entries in partition_map.items():
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
    parser.add_argument("--partitions", type=int, default=NUM_PARTITIONS)
    parser.add_argument("--vins-per-partition", type=int, default=VINS_PER_PARTITION)
    parser.add_argument("--format", choices=["txt", "csv", "json"], default=EXPORT_FORMAT)
    parser.add_argument("--filename", default=EXPORT_FILENAME)
    args = parser.parse_args()

    vin_map = generate_vins_evenly_distributed(args.partitions, args.vins_per_partition)
    all_vins = flatten_vins(vin_map)

    print(f"Generated {len(all_vins)} VINs starting with '{VIN_PREFIX}' across {args.partitions} partitions.")
    export_vins(all_vins, args.format, args.filename)
    print(f"Saved to: {args.filename}")
