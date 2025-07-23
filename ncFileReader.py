import os
import re
import xarray as xr
import pandas as pd

# Define input/output directories
input_dir = r"ClimateRecords"
output_dir = os.path.join(input_dir, "json_output")
os.makedirs(output_dir, exist_ok=True)

# Define Tampa bounding box
lat_min, lat_max = 25.0, 26.5
lon_min, lon_max = -81.5, -80.5

# Loop through each .nc file
for filename in os.listdir(input_dir):
    if filename.endswith(".nc"):
        input_path = os.path.join(input_dir, filename)

        # Extract year from filename using regex (looking for 8-digit date)
        match = re.search(r"(\d{4})\d{4}", filename)
        if match:
            year = match.group(1)
            output_filename = f"CR{year}.json"
        else:
            print(f"Could not extract year from filename: {filename}")
            continue

        output_path = os.path.join(output_dir, output_filename)

        print(f"Processing: {filename} → {output_filename}")

        try:
            with xr.open_dataset(input_path, engine="netcdf4") as ds:
                df = ds.to_dataframe().reset_index()

                # Normalize longitude to -180 to 180 if needed
                if df["longitude"].max() > 180:
                    df["longitude"] = df["longitude"].apply(lambda x: x - 360 if x > 180 else x)

                # Filter only Tampa area
                df_tampa = df[
                    (df["latitude"] >= lat_min) & (df["latitude"] <= lat_max) &
                    (df["longitude"] >= lon_min) & (df["longitude"] <= lon_max)
                ]

                # Save to JSON only if data exists
                if not df_tampa.empty:
                    df_tampa.to_json(output_path, orient="records", lines=True)
                    print(f"Saved filtered JSON to: {output_path}")
                else:
                    print(f"No Tampa-area data found in: {filename}")

        except Exception as e:
            print(f"Failed to process {filename}: {e}")
