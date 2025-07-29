# Kafka VIN Partitioning Tools

This repository contains three Python scripts designed to help generate and verify Kafka partition keys for performance and load testing. The keys are VINs (Vehicle Identification Numbers), fixed at 17 characters and starting with the prefix `TEST`.

---

## 📁 Scripts Overview

### 1. `kafka_partition_finder.py`

Calculates which Kafka partition a given key will be assigned to, using Kafka’s default Murmur2 hash strategy.

#### 🔧 Usage (CLI):
```bash
python kafka_partition_finder.py TESTVIN12345678901
```

#### 🖨 Output:
```
TESTVIN12345678901: Partition 3
```

This script also provides a reusable function:
```python
from kafka_partition_finder import kafka_partition

partition = kafka_partition("TESTVIN12345678901", num_partitions=8)
```

---

### 2. `generate_evenly_distributed_vins.py`

Generates VINs that are evenly distributed across all Kafka partitions. The VINs:
- Are 17 characters long
- Start with `TEST`
- Are saved to `.txt`, `.csv`, or `.json`

#### 🔧 Configurable parameters:
- `NUM_PARTITIONS`: Number of Kafka partitions
- `VINS_PER_PARTITION`: Number of VINs per partition
- `EXPORT_FORMAT`: One of `txt`, `csv`, or `json`

#### 🏃 Usage:
```bash
python generate_evenly_distributed_vins.py
```

#### 💾 Example Output (`vin_keys.txt`):
```
TEST7K8F1X9GZLW4V5 : Partition 0
TESTDJ6F21YUMVKPR : Partition 1
...
```

---

### 3. `verify_vin_partitions.py`

Verifies that each VIN in the generated output maps to the correct Kafka partition using `kafka_partition_finder`.

#### 🏃 Usage:
```bash
python verify_vin_partitions.py vin_keys.txt
python verify_vin_partitions.py vin_keys.csv
python verify_vin_partitions.py vin_keys.json
```

#### 🖨 Output:
```
✅ All VINs correctly map to expected partitions.
```

Or, if there are mismatches:
```
❌ Found 2 mismatches:
  VIN: TESTABC12345678901 | Expected: 4 | Actual: 2
```

---

## 🛠 Requirements

All scripts use **Python standard libraries**. No third-party packages are required.

Optional developer tools:
```txt
black==24.3.0
flake8==7.0.0
```

---

## ✅ Example Workflow

1. **Generate VINs**:
   ```bash
   python generate_evenly_distributed_vins.py
   ```

2. **Use VINs as keys** in your Kafka test system.

3. **Verify VIN-partition mapping**:
   ```bash
   python verify_vin_partitions.py vin_keys.txt
   ```

---

## 📄 License

MIT License