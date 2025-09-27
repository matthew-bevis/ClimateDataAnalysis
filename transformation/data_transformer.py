import os
import re
import xarray as xr
import pandas as pd
from utils.logger import get_logger


class DataTransformer:
    def __init__(self, lat_min, lat_max, lon_min, lon_max, output_dir):
        self.lat_min = lat_min
        self.lat_max = lat_max
        self.lon_min = lon_min
        self.lon_max = lon_max
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

        self.logger = get_logger(self.__class__.__name__)

    def process(self, nc_file):
        """
        Process a single NetCDF (.nc) file:
          - normalize longitudes > 180
          - filter by bounding box
          - write filtered parquet locally into <output_dir>/<year>/CRYYYYMMDD.parquet
        """
        try:
            # Extract full date (YYYYMMDD) from filename
            date_match = re.search(r"(\d{8})", os.path.basename(nc_file))
            if not date_match:
                self.logger.warning(f"Could not extract date from {nc_file}")
                return None

            date_str = date_match.group(1)
            year = date_str[:4]

            # Ensure year-based directory exists
            year_dir = os.path.join(self.output_dir, year)
            os.makedirs(year_dir, exist_ok=True)
            parquet_out = os.path.join(year_dir, f"CR{date_str}.parquet")

            # Open NetCDF → DataFrame
            with xr.open_dataset(nc_file, engine="netcdf4") as ds:
                df = ds.to_dataframe().reset_index()

                # Normalize longitude if > 180
                if df["longitude"].max() > 180:
                    df["longitude"] = df["longitude"].apply(
                        lambda x: x - 360 if x > 180 else x
                    )

                # Apply bounding box filter
                df_filtered = df[
                    (df["latitude"] >= self.lat_min) & (df["latitude"] <= self.lat_max) &
                    (df["longitude"] >= self.lon_min) & (df["longitude"] <= self.lon_max)
                ]

                if df_filtered.empty:
                    self.logger.warning(f"No bounding-box data found in {nc_file}")
                    return None

                # Save filtered parquet locally
                df_filtered.to_parquet(parquet_out, engine="pyarrow", index=False)

                self.logger.info(
                    f"Processed {nc_file} → {parquet_out} "
                    f"(rows={len(df_filtered)}, cols={len(df_filtered.columns)})"
                )
                return parquet_out

        except Exception as e:
            self.logger.error(f"Failed to process {nc_file}: {e}")
            return None
