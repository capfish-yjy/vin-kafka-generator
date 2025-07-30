# kafka_partition_finder.py
import argparse

def kafka_murmur2(data: bytes) -> int:
    length = len(data)
    seed = 0x9747b28c
    m = 0x5bd1e995
    r = 24

    h = seed ^ length
    length4 = length // 4

    for i in range(length4):
        i4 = i * 4
        k = (
            data[i4 + 0] & 0xff
            | ((data[i4 + 1] & 0xff) << 8)
            | ((data[i4 + 2] & 0xff) << 16)
            | ((data[i4 + 3] & 0xff) << 24)
        )
        k = (k * m) & 0xFFFFFFFF
        k ^= (k >> r) & 0xFFFFFFFF
        k = (k * m) & 0xFFFFFFFF
        h = (h * m) & 0xFFFFFFFF
        h ^= k

    # Handle remaining bytes
    remaining = length % 4
    if remaining == 3:
        h ^= (data[-1] & 0xff) << 16
        h ^= (data[-2] & 0xff) << 8
        h ^= (data[-3] & 0xff)
        h = (h * m) & 0xFFFFFFFF
    elif remaining == 2:
        h ^= (data[-1] & 0xff) << 8
        h ^= (data[-2] & 0xff)
        h = (h * m) & 0xFFFFFFFF
    elif remaining == 1:
        h ^= (data[-1] & 0xff)
        h = (h * m) & 0xFFFFFFFF

    h ^= (h >> 13) & 0xFFFFFFFF
    h = (h * m) & 0xFFFFFFFF
    h ^= (h >> 15) & 0xFFFFFFFF

    return h

def kafka_partition(key: str, num_partitions: int) -> int:
    hash_val = kafka_murmur2(key.encode('utf-8'))
    partition = (hash_val & 0x7fffffff) % num_partitions
    return partition

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Find Kafka partition for VIN keys.")
    parser.add_argument("--partitions", type=int, default=16, help="Number of partitions")
    parser.add_argument("--vins", nargs="+", required=True, help="List of VIN keys")
    args = parser.parse_args()

    for key in args.vins:
        partition = kafka_partition(key, args.partitions)
        print(f"{key}: Partition {partition}")
