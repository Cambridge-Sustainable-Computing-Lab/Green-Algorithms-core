# Green-Algorithms-core

![Version: v0.1.0](https://img.shields.io/badge/version-v0.1.0-orange)
[![Open Source? Yes!](https://badgen.net/badge/Open%20Source%20%3F/Yes%21/purple?icon=github)](https://github.com/Naereen/badges/)

This package implements the [**Green Algorithms** methodology](https://doi.org/10.1002/advs.202100707) to estimate component-wise energy consumption and carbon footprints of computational workloads. 

> [!IMPORTANT]
> This package is open-source and directly usable, but is mainly intended as a base package to be used by other Green Algorithms tools.

Divided primarily into two pipelines—**Ingestion** and **Computation**—it processes raw job logs extracted directly from workload managers (such as SLURM) or structured CSV files. The pipeline produces aggregated output per user and per day. Additionally, the computation module exposes underlying tools to calculate energy consumption, carbon footprints, and carbon emission equivalents directly.

> ⚠️ **Status:** This package is in its early developmental stages and is expected to evolve significantly.

---

## 🌱 Features

* **Flexible Ingestion:** Extract logs from SLURM using sacct (with provision to extend for other workload managers) or structured logs in CSV format.
* **Energy Calculation:** Models power draw based on CPU/GPU architecture, core counts, memory usage, and runtime.
* **Carbon Footprint Calculation:** Uses [CarbonIntensity regional API](https://carbonintensity.org.uk) (UK only) to integrate dynamic carbon intensity (CI) with estimated energy consumption.  
* **Contextual Metrics:** Translates raw $\text{CO}_2\text{e}$ metrics into intuitive real-world equivalents (e.g., tree-months for sequestration, car miles driven).
* **Storage Backend Agnostic:** Supports custom storage backends (via `CIStorageBackend`) to cache or persist carbon intensity values.

---

## 📋 Requirements

* **Python:** `>= 3.11`
* **Core Dependencies:**
  * `pandas`
  * `numpy`
  * `requests`

---

## 📥 Installation

Currently, `Green-Algorithms-core` can be installed directly from this Git repository using `pip`.

### Standard Installation
```bash
pip install git+https://github.com/Cambridge-Sustainable-Computing-Lab/Green-Algorithms-core.git
```

### Specific Release or Branch
```bash
pip install git+https://github.com/Cambridge-Sustainable-Computing-Lab/Green-Algorithms-core.git@v0.1.0
pip install git+https://github.com/Cambridge-Sustainable-Computing-Lab/Green-Algorithms-core.git@main
```

## 🚀 Quickstart

💡 Refer to [tests/conftest.py](tests/conftest.py) for examples of required configuration parameters.

```python
import pandas as pd
from ga_core import HPCDataProcessor

# Define Configurations and Fixed Parameters
cluster_info = {
    # Add cluster-specific information here...
}

config_data = {
    # Add main configurations here...
}

fixed_params = {
    # Add hardware constants and baseline parameters here...
}

# Initialize the Processor
processor = HPCDataProcessor(
    config_data=config_data,
    cluster_info=cluster_info,
    fixed_params=fixed_params,
    all_users_access=False # Set to True if you have access to multiple user's logs
)

# Extract and Enrich Data
raw_df = processor.extract_data()
enriched_df = processor.enrich_data(raw_df)

# View Results
print(enriched_df[["UserX", "StartDatetimeX", "carbonFootprint", "energy_CPUs", "energy_GPUs"]].head())
```

### Read raw logs from a file
You can bypass direct interaction with the workload manager by reading raw logs from a file instead. `HPCDataProcessor.extract_data()` accepts raw log bytes and handles cleaning and preparation for the enrichment step.

```python
with open('sample_raw_logs.txt', 'rb') as f:
    logs_raw = f.read()  # Read raw logs

# Extract and enrich data
raw_df = processor.extract_data(logs_raw)
enriched_df = processor.enrich_data(raw_df)
```
## ⚒️ Development Installation and Tests

If you are contributing to or extending the codebase locally:
```bash
# Clone the repository
git clone https://github.com/Cambridge-Sustainable-Computing-Lab/Green-Algorithms-core.git
cd Green-Algorithms-core

# Install in editable mode with development dependencies
pip install -e ".[dev]"
````

💡 Using .[dev] installs the package in editable mode along with testing tools like pytest.

### Running tests

Execute the test suite to verify your local setup:

```bash
pytest .
```

### Contributing

1. **Fork** the repository and clone your fork locally.
2. Create a new branch off `main` for your change:
````bash
git checkout main
git checkout -b feature/<your-feature-name>-<your-username>
````
3. Make your changes, then run `pytest .` to make sure nothing's broken.
4. Commit your changes with a clear message, push to your fork, and open a **Pull Request against `main`**.

> [!IMPORTANT]
> Please open an issue for larger changes.

### Reference & Test Data
The `tests/` directory also serves as a reference for inputs, configurations, and expected pipeline outputs:
- Pipeline Examples: Check `tests/test_hpc_data_pipeline.py` to see how the pipeline handles completed, failed, and unfinished jobs.
- Sample Data: Inspect `tests/testdata/slurm/` for raw SLURM log fixtures and their corresponding golden CSV outputs.
- Fixture Configurations: Refer to `tests/conftest.py` for baseline configuration dictionaries (cluster_info, config_data, fixed_params).

---
## About us

This package is built and maintained by the [Cambridge Sustainable Computing Lab](https://cam-sustainablecomputing.org) at the University of Cambridge, UK. 

---
## Licence

[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

This work is licensed under the [GNU General Public License v3.0](https://www.gnu.org/licenses/gpl-3.0).
