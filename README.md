# NHANES extractor

Download NHANES and extract codes

---

## Usage

1. Download all NHANES datasets and store them as xz compressed CSV files

`python3 download.py <path_to_NHANES_data>`

2. Get datasets by their codes (one code per line) and merge them into a single `csv` file

`python3 get_records.py <path_to_NHANES_data> <file_with_codes> <output_csv_file>`
