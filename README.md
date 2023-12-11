# Colibri Wind Turbine Data Pipeline
This projects the following installations:
Java
Python
Pyspark

Once the applications are installed, start up pyspark shell and execute this file.

## Objective
The Wind Turbine Data Pipeline is designed to process, clean, impute missing values, compute summary statistics, identify anomalies, and separate regular and abnormal data from wind turbine datasets. The primary objective is to ensure data completeness, accuracy, and facilitate further analysis.

## Functional Requirements

### Data Processing
- Load data from CSV files located at specified paths.
- Clean the loaded data by filtering out records with missing timestamps or turbine IDs.
- Convert timestamp to date format for downstream processing.

### Data Imputation
- Substitute missing power outputs for each turbine with the daily average power output of that turbine.

### Summary Statistics
- Compute minimum, maximum, average, and standard deviation of power output for each turbine on a daily basis.

### Anomaly Detection
- Identify and separate regular data (within 2 standard deviations of daily average) and abnormal data (outside 2 standard deviations of daily average) for further analysis.

### Data Output
- Display a sample of bad data (records with missing timestamps or turbine IDs).
- Show the imputed data after missing values substitution.
- Present computed summary statistics for turbines.
- Output regular and abnormal data for further analysis.

## Technology Stack
- Language: Python
- Libraries: PySpark

## Usage
- Initialize a Spark session and load data from CSV files.
- Clean the loaded data, impute missing values, compute summary statistics, and identify anomalies.
- Display sample data and store results in a database.

## Dependencies
- pyspark
- functools

## Usage Instructions
1. Ensure the PySpark environment is set up.
2. Run the provided Python script to execute the Wind Turbine Data Pipeline.
3. Customize file paths for data loading as needed.
4. Adjust database connection settings for data storage.

## Contributors
- [Your Name]
- [Other Contributors]

## License
[Specify License]

