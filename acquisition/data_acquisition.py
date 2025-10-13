import os
import re
import urllib.parse
import xml.etree.ElementTree as ET
import requests
from utils.logger import get_logger

NAMESPACE = "http://s3.amazonaws.com/doc/2006-03-01/"  # kept for reference/logs


class DataAcquisition:
    def __init__(self, base_url, output_dir):
        self.base_url = base_url.rstrip("/")
        self.output_dir = output_dir
        self.logger = get_logger(self.__class__.__name__)
        os.makedirs(self.output_dir, exist_ok=True)

    # ------------------------ internal helpers ------------------------

    @staticmethod
    def _strip_ns(tag: str) -> str:
        """Return the local tag name without the '{ns}' prefix, if any."""
        if "}" in tag:
            return tag.split("}", 1)[1]
        return tag

    def _parse_listing_xml(self, xml_bytes):
        """
        Parse a single S3 ListBucketResult XML page, namespace-agnostic.
        Returns (objects, next_token) where:
          - objects: list of dicts {key,size,last_modified}
          - next_token: str | None
        """
        try:
            root = ET.fromstring(xml_bytes)
        except ET.ParseError:
            # Malformed XML — be tolerant and return empty page
            return [], None

        objs = []
        next_token = None

        # Find 'Contents' elements regardless of namespace
        for elem in root.iter():
            if self._strip_ns(elem.tag) != "Contents":
                continue

            key = size = last = None
            for child in elem:
                name = self._strip_ns(child.tag)
                if name == "Key":
                    key = (child.text or "").strip()
                elif name == "Size":
                    try:
                        size = int((child.text or "0").strip())
                    except ValueError:
                        size = 0
                elif name == "LastModified":
                    last = (child.text or "").strip()

            if key and key.lower().endswith(".nc"):
                if "AVHRR-Land" in key or "VIIRS-Land" in key:
                    objs.append({"key": key, "size": size or 0, "last_modified": last or ""})
                else:
                    self.logger.warning("Skipping .nc without AVHRR/VIIRS in key: %s", key)
                    
        # Find NextContinuationToken if present
        for elem in root.iter():
            if self._strip_ns(elem.tag) == "NextContinuationToken":
                if elem.text:
                    next_token = elem.text.strip() or None
                break

        return objs, next_token

    def _list_objects(self):
        """Return list of dicts: {key, size, last_modified} for relevant NOAA .nc files."""
        self.logger.info("Fetching file list from S3 with sizes")
        entries, token = [], None

        while True:
            url = f"{self.base_url}/?list-type=2&prefix=data/&max-keys=1000"
            if token:
                url += f"&continuation-token={urllib.parse.quote(token)}"

            # HTTP fetch
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()

            # Parse this page
            page_objs, token = self._parse_listing_xml(resp.content)
            entries.extend(page_objs)

            if not token:
                break

        self.logger.info("Total .nc candidates found: %d", len(entries))
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
            "Planning %d files across %d days/year (cap %.1f GB). Estimated: %.2f GB",
            len(capped),
            per_year_days,
            max_total_bytes / (1024**3),
            gb,
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
                self.logger.info("Already have %s, skipping download.", nc_path)
                return nc_path

            self.logger.info("Downloading %s → %s", url, nc_path)
            with requests.get(url, stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(nc_path, "wb") as f:
                    for chunk in r.iter_content(1024 * 1024):
                        if chunk:
                            f.write(chunk)

            return nc_path

        except Exception as e:
            self.logger.error("Failed to download %s: %s", key, e)
            return None
