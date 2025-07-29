# kafka_partition_finder.py
import sys

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
    if len(sys.argv) < 2:
        print("Usage: python kafka_partition_finder.py [num_partitions] key1 key2 key3 ...")
        sys.exit(1)

    try:
        num_partitions = int(sys.argv[1])
        keys = sys.argv[2:]
        if not keys:
            print("Please provide at least one key.")
            sys.exit(1)
    except ValueError:
        num_partitions = 16
        keys = sys.argv[1:]

    for key in keys:
        partition = kafka_partition(key, num_partitions)
        print(f"{key}: Partition {partition}")
