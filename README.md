# Climate Data Analysis Pipeline
This project prototypes a data engineering pipeline for acquiring, transforming, and storing climate datasets from NOAA’s Leaf Area Index (LAI) and Fraction of Absorbed Photosynthetically Active Radiation (FAPAR) Climate Data Record (CDR).

The pipeline is designed to support exploratory analysis of climate change impacts on vegetation health by automating the ingestion and filtering of large-scale satellite datasets into an analysis-ready format.

---

## Background
The NOAA LAI & FAPAR CDR combines satellite observations into a long-term, consistent dataset of two key biophysical variables:

 - Leaf Area Index (LAI)
Tracks the one-sided green leaf area per unit of ground surface area. It is widely used to monitor vegetation growth, productivity, and canopy structure.

 - Fraction of Absorbed Photosynthetically Active Radiation (FAPAR)
Quantifies the solar radiation absorbed by plants in the PAR spectral region (400–700 nm). It is a core variable for photosynthesis modeling, carbon cycle studies, and drought/vegetation stress monitoring.

These variables provide essential inputs for:

 - Climate change impact assessments

 - Vegetation stress monitoring

 - Forecasting agricultural yields

 - Hydrology and ecosystem modeling

 - Natural resource management

 ## Project Goals
  - Automate data acquisition from NOAA’s public S3 bucket.

 - Filter datasets by geographic bounding box (e.g. Tampa Bay, Florida Everglades).

 - Transform NetCDF (.nc) files into Parquet format (optimized for big data analytics).

 - Store results in Azure Data Lake Storage (ADLS) as blobs for scalable analysis.

 - Implement checkpointing to skip already-processed files.

 - Prototype orchestration with Airflow, paving the way for scheduled ingestion of new data.

 - Log metrics on pipeline performance (files, rows, columns, runtime).

 - Enable distributed data processing with Apache Spark and Azure Databricks.

 ## Tech Stack
 - Python (pandas, xarray, netCDF4) — data parsing and transformation

 - Azure Blob Storage / Data Lake — cloud storage for processed datasets

 - Apache Airflow — workflow orchestration (prototype DAG provided)

 - Apache Spark (PySpark) - for task and query execution

 - Azure DataBricks - Spark cluster environment for queries and analytics

 - Logging — pipeline logs (pipeline.log)

 - Git — version control

 - NOAA S3 Bucket — raw data source (noaa-cdr-leaf-area-index-fapar-pds.s3.amazonaws.com)

 ## Project Structure
 ```bash
 ClimateDataAnalysis/
├── acquisition/           # Data acquisition from NOAA S3
│   └── data_acquisition.py
├── transformation/        # Filtering & transformation of NetCDF to Parquet
│   └── data_transformer.py
├── storage/               # Upload to Azure Data Lake Storage
│   └── data_storage.py
├── pipeline/              # Orchestrator
│   └── climate_pipeline.py
├── utils/                 # Logging & checkpoint helpers
│   ├── logger.py
│   └── checkpoint.py
├── dags/                  # Example Airflow DAG
│   └── climate_pipeline_daily_dag.py
├── ClimateRecords/        # Local download cache (ignored in Git)
├── requirements.txt       # Python dependencies
├── .gitignore             # Ignore data, logs, envs
└── README.md
 ```

 ## Setup
This Project runs from within a WSL2 instance with an installation of 
 1. Start your virtual environment
 ```bash
python -m venv venv
 ```
 2. Install dependencies
 ```bash
 pip install -r requirements.txt
 ```
 2. Configure Azure storage (via .env file)
 ```bash
AZURE_STORAGE_ACCOUNT=<your-account-name>
AZURE_STORAGE_KEY=<your-storage-key>
AZURE_BLOB_CONTAINER=<your-blob-container>
 ```
 3. Run the pipeline
 ```bash
 spark-submit ./pipeline/climate_pipeline.py
 ```
By default, it selects the first N days of each year (configurable) and caps the total downloaded size (e.g. ~3 GB).

4. Check results in Azure Data Lake (example with Azure CLI)
```bash
az storage fs file list -f climate-data-analysis --account-name <your-account> --output table
```

## Next Steps
 - Expand data acquisition to reach a more substantial quantity. (~10 GB)
 - Perform exploratory data analysis on the vegetation stress trends.
 - Run queries on the full dataset using Apache Spark on Databricks.
 - Develop dashboards and visualizations of vegetation stress over time.