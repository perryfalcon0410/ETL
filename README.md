# ETL Pipeline Project

## Project Overview
This project demonstrates an **ETL (Extract, Transform, Load)** pipeline built in Python. It automatically downloads data from various sources (Google Sheets, CSV, MySQL, HTML tables), performs necessary transformations, and then merges the data into a final dataset for analysis.

The purpose of this project is to demonstrate how to automate data extraction, clean and transform data, and load it into a unified format that can be used for further analytics.

## Data Sources
The data used in this project comes from the following sources:
1. **Google Sheets**: Enrollee names and other information.
2. **CSV Files**: 
    - `enrollies_education.csv` contains education data.
    - `work_experience.csv` contains work experience data.
3. **MySQL Database**: Training hours of enrollees.
4. **HTML Table**: City development index from a website.

## Data Cleaning and Transformation
### Steps and Decisions
1. **Filling Missing Values**: 
   - In the `gender` column, missing values were filled with the string 'Other'. This decision ensures that missing values do not affect the analysis.
   
2. **Merging Data**:
   - Data was merged based on common identifiers (`enrollee_id` or `city`), ensuring that all relevant information for each enrollee was combined.
   
3. **Standardizing Data**:
   - Column names from the city development index table were converted to lowercase to maintain consistency when merging with other datasets.

These transformations ensure that the data is clean, free from missing values, and properly structured for analysis.

## How to Run the ETL Script
The ETL pipeline is implemented in Python as `etl.py`. To run the script:

1. **Install Required Libraries**:
   Install the required libraries using pip:
   ```bash
   pip install pandas mysql-connector-python requests lxml
   ```
2. **Download and Run the Script**:
   Save the script as `etl.py` and run it using Python:
   ```bash
   python etl.py
   ```
3. **Output**: The script will output the combined dataset to the console, displaying the first 5 rows of the final merged DataFrame.

### Scheduling the Script
You can schedule the ETL pipeline to run automatically at a regular interval. Below are instructions for scheduling it on different platforms.

### Scheduling on Windows (Using Task Scheduler)
1. Open **Task Scheduler** on your Windows machine.
2. Click on **Create Task** in the Actions pane.
3. In the **General** tab, give your task a name, such as "ETL Pipeline".
4. In the **Triggers** tab, click on **New** and set the schedule (e.g., daily or weekly).
5. In the **Actions** tab, click on **New**, and set the following:
   - Action: **Start a program**
   - Program/Script: `python`
   - Add arguments: `etl.py` (ensure the path to `etl.py` is correctly set if the script is not in the current working directory).
6. Save the task, and it will run automatically based on the schedule.

### Scheduling on Linux (Using Cron)
1. Open a terminal.
2. Edit the cron jobs by typing:
   ```bash
   crontab -e
   ```
3. Add the following line to schedule the script to run daily at 2 AM:
   ```bash
    0 2 * * * /usr/bin/python3 /path/to/etl.py
   ```
4. Save and exit the editor. The script will now run daily at the specified time.

### Conclusion
This project illustrates the basic steps of setting up an ETL pipeline. It automates the download, transformation, and merging of datasets from various sources. The final output provides a consolidated dataset that can be used for further analysis or reporting.
