# F1 Data Engineering Project

![alt text](image-1.png)

This project aims to extract, process, and store (ETL) Formula 1 data using the Ergast API for subsequent analysis. The data is stored in Parquet format in a Data Lake. The raw extracted data is stored in the bronze layer of the datalake and subsequently processed and stored in an OLAP database (PostgreSQL in Aiven) for further analysis. The transformed/refined data is also stored in the silver layer of the datalake for the same purpose. This project is part of my integrative practical work in the Data Engineering course provided by the National Technological University (UTN) of Argentina.

## Data Sources

The data is sourced from the Ergast API, which provides historical and current data for Formula 1 races, drivers, and teams.

## Project Overview

The project involves the following steps:

1. Extraction of F1 data from the Ergast API.
2. Storage of raw data in the bronze layer of the datalake.
3. Processing and transformation of the data.
4. Storage of transformed data in an OLAP PostgreSQL database and in the silver layer of the datalake for analysis.

## Extracted Data

1. Data of all drivers in F1 history.
2. Race results, available at the end of each race.
3. Lap times: time taken by each driver to complete each lap in every race of the season.
4. Driver championship statistics: points accumulated by each driver after each race.

The functions used for these extractions are in `utils_db.py`.

**Storage in Parquet Format:**

- **Directory Creation:** Ensures specified directories exist; they are created automatically if not.
- **Data Storage:** Data is stored in Parquet format in the bronze layer of the datalake. Temporary data like lap times are partitioned by season, race, and driver. Other data is partitioned by season and race.

## Transformations Applied

The following base transformations have been applied to the four tables (`drivers`, `lap_times`, `race_results`, `driver_standings`) as appropriate:

1. **Duplicate removal.**
2. **Null value removal.**
3. **Column type conversion for optimization.**
4. **Column renaming.**
5. **Formatting of date type columns.**

### Lap Times Table (`lap_times`)

Creation of two new columns using `groupby`:
1. **Driver's Fastest Lap:** The best lap time of a driver in the race.
2. **Race Fastest Lap:** The best lap time of all drivers in the race.

(Note: The driver with the fastest lap in a race earns an extra championship point.)

### Driver Standings Table (`driver_standings`)

Addition of three new columns:

1. **Total Wins:** Calculated from the `race_results` table and merged into the standings table.
2. **Total Podiums:** Calculated from the `race_results` table and merged into the standings table.
3. **Total Fastest Laps:** Calculated from the `lap_times` table and merged into the standings table.

## Load/Storage of OLAP Data

The connection to a PostgreSQL database is established, and the transformed/refined data is stored in specific tables. Additionally, the transformed/refined data is stored in the silver layer of the datalake.

## Libraries Used

- Pandas: For data manipulation and analysis.
- PyArrow: For reading and writing Parquet files.
- SQLAlchemy: For interacting with SQL databases, such as PostgreSQL.
- Requests: For making HTTP requests to the Ergast API.
- OS: For handling file paths and directory creation.
- Datetime: For date and time manipulation.

## Content

- `requirements.txt`: Project dependencies list.
- `etl_pipeline.ipynb`: Jupyter notebooks with all the work (data extraction, processing, and loading).
- `utils_db.py`: Functions for extracting necessary data from the Ergast API.
- `datalake/`: Storage for raw data in the bronze layer and refined data in the silver layer after executing `etl_pipeline.ipynb`.

## Setup Instructions

1. Clone the repository:
    ```sh
    git clone https://github.com/lucianoalessi/ETL-Formula1-DataPipeline.git
    ```

2. Navigate to the project directory:
    ```sh
    cd ETL-Formula1-DataPipeline
    ```

3. Create a virtual environment to install dependencies:
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate.bat`
    ```

4. Install the required dependencies:
    ```sh
    pip install -r requirements.txt
    ```

5. Set up the PostgreSQL database and configure the connection details:

    Create a configuration file named `pipeline.conf` in the project directory (`ETL-Formula1-DataPipeline`). This file should contain the connection details and PostgreSQL credentials. These details will be used by the `connect_to_db` function to establish the connection with the database.

    Template for creating the file and entering connection data:
    ```ini
    [postgres]
    host=****.aivencloud.com
    port=15191
    user=avnadmin
    pwd=****
    dbname=defaultdb
    ```

6. Run the Jupyter notebook to execute the ETL pipeline:
    ```sh
    jupyter notebook etl_pipeline.ipynb
    ```

## Usage Instructions

1. Run the ETL pipeline by executing the main script:
    ```sh
    python etl_pipeline.py
    ```

2. Monitor the logs for any errors or issues during execution.
3. Access the stored data in the datalake or the PostgreSQL database for analysis.

## Output

The output includes:

- Raw data stored in Parquet format in the bronze layer of the datalake.
- Transformed data stored in Parquet format in the silver layer of the datalake.
- Transformed data stored in an OLAP PostgreSQL database.

## Contributions

Contributions are welcome. Please open an issue to discuss any major changes before making them.

## License

This project is licensed under the MIT License.

