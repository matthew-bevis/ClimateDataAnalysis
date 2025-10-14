import os
import re
import xarray as xr
import pandas as pd
import numpy as np
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

    def _epoch_ms_from_date_str(self, yyyymmdd: str) -> int:
        # Midnight UTC for the file's date
        ts = pd.to_datetime(yyyymmdd, format="%Y%m%d", utc=True)
        return int(ts.value // 1_000_000)  # ns -> ms 

    def _coerce_time_to_ms(self, s: pd.Series, fallback_ms: int) -> pd.Series:
        """
        Ensure 'time' becomes int64 epoch-ms.
        Handles datetime64, strings, ints/floats; fills missing with fallback.
        """
        if s is None or s.empty:
            return pd.Series(fallback_ms, index=(s.index if s is not None else None), dtype="int64")

        # Datetime-like series
        if np.issubdtype(s.dtype, np.datetime64):
            # Normalize to ns then convert to ms
            return (s.astype("datetime64[ns]").astype("int64") // 1_000_000).astype("int64")

        # Strings → datetime (UTC) → ms
        if s.dtype == object:
            dt = pd.to_datetime(s, utc=True, errors="coerce")
            if dt.notna().any():
                return (dt.astype("int64") // 1_000_000).fillna(fallback_ms).astype("int64")

        # Numeric seconds/ms → ms (heuristic)
        if np.issubdtype(s.dtype, np.integer) or np.issubdtype(s.dtype, np.floating):
            ss = pd.to_numeric(s, errors="coerce")
            is_ms = ss > 100_000_000_000  # > ~1973 in ms
            ms = np.where(is_ms, ss, ss * 1000.0)
            return pd.Series(ms, index=ss.index).fillna(fallback_ms).astype("int64")

        # Fallback: constant midnight for this file
        return pd.Series(fallback_ms, index=s.index, dtype="int64")

    def process(self, nc_file):
        try:
        # Extract full date (YYYYMMDD) from filename
            date_match = re.search(r"(\d{8})", os.path.basename(nc_file))
            if not date_match:
                self.logger.warning(f"Could not extract date from {nc_file}")
                return None

            date_str = date_match.group(1)
            year = date_str[:4]
            midnight_ms = self._epoch_ms_from_date_str(date_str)

            year_dir = os.path.join(self.output_dir, year)
            os.makedirs(year_dir, exist_ok=True)
            parquet_out = os.path.join(year_dir, f"CR{date_str}.parquet")

            with xr.open_dataset(nc_file, engine="netcdf4") as ds:
                # Convert xarray to pandas DataFrame
                df = ds.to_dataframe().reset_index()

                # Normalize longitude if > 180
                if "longitude" in df.columns and df["longitude"].max() > 180:
                    df["longitude"] = df["longitude"].apply(lambda x: x - 360 if x > 180 else x)

                # Bounding box filter
                must_cols = {"latitude", "longitude"}
                if not must_cols.issubset(df.columns):
                    self.logger.warning(f"Missing required coords in {nc_file}: have {list(df.columns)}")
                    return None

                mask = (
                    (df["latitude"] >= self.lat_min)
                    & (df["latitude"] <= self.lat_max)
                    & (df["longitude"] >= self.lon_min)
                    & (df["longitude"] <= self.lon_max)
                )

                keep_cols = [c for c in ("latitude", "longitude", "LAI", "FAPAR", "time") if c in df.columns]
                df_filtered = df.loc[mask, keep_cols].copy()

                if df_filtered.empty:
                    self.logger.warning(f"No bounding-box data found in {nc_file}")
                    return None

                if "time" in df_filtered.columns:
                    ms = self._coerce_time_to_ms(df_filtered["time"], midnight_ms)  # int64 Series
                    # Ensure index aligns, then drop and reinsert as pure int64
                    ms = ms.reindex(df_filtered.index).astype("int64")
                    df_filtered = df_filtered.drop(columns=["time"], errors="ignore")
                    df_filtered.loc[:, "time"] = ms
                else:
                    df_filtered.loc[:, "time"] = np.int64(midnight_ms)

                # Coerce numeric columns
                for col in ("latitude", "longitude", "LAI", "FAPAR"):
                    if col in df_filtered.columns:
                        df_filtered.loc[:, col] = pd.to_numeric(df_filtered[col], errors="coerce").astype("float64")

                out_cols = [c for c in ("latitude", "longitude", "time", "LAI", "FAPAR") if c in df_filtered.columns]
                df_out = df_filtered.loc[:, out_cols]

                # Save Parquet
                df_out.to_parquet(parquet_out, engine="pyarrow", index=False)

                self.logger.info(
                    f"Processed {nc_file} → {parquet_out} "
                    f"(rows={len(df_out)}, cols={len(df_out.columns)})"
                )
                return parquet_out

        except Exception as e:
            self.logger.error(f"Failed to process {nc_file}: {e}")
            return None