import os
import re
import urllib.parse
import xml.etree.ElementTree as ET
import requests
from utils.logger import get_logger

NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"


class DataAcquisition:
    def __init__(self, base_url, output_dir):
        self.base_url = base_url
        self.output_dir = output_dir
        self.logger = get_logger(self.__class__.__name__)
        os.makedirs(self.output_dir, exist_ok=True)

    def _list_objects(self):
        """Return list of dicts: {key, size, last_modified} for all NOAA .nc files we care about."""
        self.logger.info("Fetching file list from S3 with sizes")
        entries, token = [], None
        while True:
            url = f"{self.base_url}/?list-type=2&prefix=data/&max-keys=1000"
            if token:
                url += f"&continuation-token={urllib.parse.quote(token)}"
            resp = requests.get(url)
            resp.raise_for_status()
            root = ET.fromstring(resp.content)

            for c in root.findall(f".//{{{NAMESPACE}}}Contents"):
                key = c.find(f"{{{NAMESPACE}}}Key").text
                if not (key.endswith(".nc") and ("AVHRR-Land" in key or "VIIRS-Land" in key)):
                    continue
                size = int(c.find(f"{{{NAMESPACE}}}Size").text)
                lm = c.find(f"{{{NAMESPACE}}}LastModified").text
                entries.append({"key": key, "size": size, "last_modified": lm})

            nxt = root.find(f"{{{NAMESPACE}}}NextContinuationToken")
            token = nxt.text if nxt is not None else None
            if not token:
                break

        self.logger.info(f"Total .nc candidates found: {len(entries)}")
        return entries

    def get_first_n_days_per_year_capped(
        self, per_year_days=3, years=range(1981, 2026), max_total_bytes=3 * (1024**3)
    ):
        """
        Choose earliest `per_year_days` per year, stop when total size > cap.
        Returns list of dicts: {key, size, year, date_str}
        """
        all_objs = self._list_objects()
        per_year = {y: [] for y in years}

        for obj in all_objs:
            m = re.search(r"(\d{8})", obj["key"])
            if not m:
                continue
            date_str = m.group(1)
            year = int(date_str[:4])
            if year not in per_year:
                continue
            per_year[year].append({**obj, "date_str": date_str, "year": year})

        selected = []
        for y in sorted(per_year.keys()):
            per_year[y].sort(key=lambda d: d["date_str"])
            selected.extend(per_year[y][:per_year_days])

        selected.sort(key=lambda d: (d["year"], d["date_str"]))
        capped, running = [], 0
        for rec in selected:
            if running + rec["size"] > max_total_bytes:
                break
            capped.append(rec)
            running += rec["size"]

        gb = running / (1024**3)
        self.logger.info(
            f"Planning {len(capped)} files across {per_year_days} days/year "
            f"(cap {max_total_bytes/(1024**3):.1f} GB). Estimated: {gb:.2f} GB"
        )
        return capped

    def download(self, key):
        """
        Download a .nc file from S3 into the local output dir and return the path.
        """
        try:
            url = f"{self.base_url}/{key}"
            filename = os.path.basename(key)

            # Place into year-based folder
            m = re.search(r"(\d{8})", filename)
            year = m.group(1)[:4] if m else "unknown"
            year_dir = os.path.join(self.output_dir, year)
            os.makedirs(year_dir, exist_ok=True)

            nc_path = os.path.join(year_dir, filename)

            if os.path.exists(nc_path):
                self.logger.info(f"Already have {nc_path}, skipping download.")
                return nc_path

            self.logger.info(f"Downloading {url} → {nc_path}")
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(nc_path, "wb") as f:
                    for chunk in r.iter_content(1024 * 1024):
                        f.write(chunk)

            return nc_path

        except Exception as e:
            self.logger.error(f"Failed to download {key}: {e}")
            return None
