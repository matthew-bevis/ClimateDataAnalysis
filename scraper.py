import os
import re
import requests
import subprocess
import xml.etree.ElementTree as ET
import urllib.parse
from datetime import datetime

# Config
BASE_URL = "https://noaa-cdr-leaf-area-index-fapar-pds.s3.amazonaws.com"
CLIMATE_DIR = "ClimateRecords"
JSON_OUTPUT_DIR = os.path.join(CLIMATE_DIR, "json_output")
YEARS = range(1982, 2026)
NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"

os.makedirs(CLIMATE_DIR, exist_ok=True)
os.makedirs(JSON_OUTPUT_DIR, exist_ok=True)

def get_nc_files_from_s3():
    print("Fetching full S3 file list (with pagination)...")
    keys = []
    continuation_token = None

    while True:
        url = f"{BASE_URL}/?list-type=2&prefix=data/&max-keys=1000"
        if continuation_token:
            encoded = urllib.parse.quote(continuation_token)
            url += f"&continuation-token={encoded}"

        resp = requests.get(url)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)

        contents = root.findall(f".//{{{NAMESPACE}}}Contents")
        for c in contents:
            key = c.find(f"{{{NAMESPACE}}}Key").text
            if key.endswith(".nc") and ("AVHRR-Land" in key or "VIIRS-Land" in key):
                keys.append(key)

        token_el = root.find(f"{{{NAMESPACE}}}NextContinuationToken")
        if token_el is not None:
            continuation_token = token_el.text
        else:
            break

    print(f"Total .nc keys found: {len(keys)}")
    return keys

def find_earliest_per_year(keys):
    candidates = {}

    for key in keys:
        match = None

        # Try AVHRR or VIIRS-style patterns
        if "AVHRR-Land" in key:
            match = re.search(r"NOAA-\d+_(\d{8})_c", key)
        elif "VIIRS-Land" in key:
            # Be more flexible for VIIRS: look for _YYYYMMDD_c anywhere
            match = re.search(r"_(\d{8})_c", key)

        if not match:
            continue

        date_str = match.group(1)
        try:
            date = datetime.strptime(date_str, "%Y%m%d")
            year = date.year
            if year in YEARS:
                if year not in candidates or date < candidates[year][0]:
                    candidates[year] = (date, key)
        except ValueError:
            continue

    return {year: info[1] for year, info in sorted(candidates.items())}


def download_and_process(key, year):
    url = f"{BASE_URL}/{key}"
    nc_path = os.path.join(CLIMATE_DIR, os.path.basename(key))
    json_path = os.path.join(JSON_OUTPUT_DIR, f"CR{year}.json")

    print(f"Downloading: {url}")
    with requests.get(url, stream=True) as resp:
        resp.raise_for_status()
        with open(nc_path, "wb") as fw:
            for chunk in resp.iter_content(1024 * 1024):
                fw.write(chunk)

    print(f"Processing: {nc_path} → {json_path}")
    subprocess.run(["python", "ncFileReader.py", nc_path, json_path], check=True)
    os.remove(nc_path)
    print(f"Deleted: {nc_path}")

def main():
    keys = get_nc_files_from_s3()
    earliest = find_earliest_per_year(keys)

    print("\nMatched candidates:")
    for year in sorted(earliest):
        print(f"{year} → {earliest[year]}")

    for year in sorted(earliest):
        try:
            download_and_process(earliest[year], year)
        except Exception as e:
            print(f"Failed {year}: {e}")

if __name__ == "__main__":
    main()
