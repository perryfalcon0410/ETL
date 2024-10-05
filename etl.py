import pandas as pd
import mysql.connector
import requests
import os

# Function to download a file from a URL
def download_file(url, filename):
    """
    Downloads a file from a given URL and saves it locally.
    """
    response = requests.get(url)
    with open(filename, 'wb') as f:
        f.write(response.content)
    print(f"Downloaded: {filename}")

# Function to read a CSV file
def read_csv_file(csv_url, description):
    """
    Reads a CSV file from a URL or local path, and handles any errors.
    """
    try:
        df = pd.read_csv(csv_url)
        print(f"{description} - First 5 rows:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error reading CSV file ({description}): {e}")
        return None

# Function to read a Google Sheet and convert to CSV
def read_google_sheet(sheet_url):
    """
    Converts a Google Sheet URL to a downloadable CSV URL and reads it.
    """
    sheet_id = sheet_url.split('/')[5]
    sheet_url_csv = f'https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv'
    try:
        df = pd.read_csv(sheet_url_csv)
        print("Google Sheet - First 5 rows:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error reading Google Sheet: {e}")
        return None

# Function to read a MySQL table
def read_mysql_table(query, connection_params, description):
    """
    Connects to a MySQL database and retrieves data using a query.
    """
    try:
        mydb = mysql.connector.connect(**connection_params)
        mycursor = mydb.cursor()
        mycursor.execute(query)
        result = mycursor.fetchall()
        df = pd.DataFrame(result, columns=[i[0] for i in mycursor.description])
        print(f"{description} - First 5 rows:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error reading MySQL table ({description}): {e}")
        return None

# Function to read an HTML table
def read_html_table(url, description):
    """
    Reads the first table from an HTML page and returns it as a DataFrame.
    """
    try:
        tables = pd.read_html(url)
        df = tables[0]  # Assuming the relevant data is in the first table
        print(f"{description} - First 5 rows:")
        print(df.head())
        return df
    except Exception as e:
        print(f"Error reading HTML table ({description}): {e}")
        return None

# Extract Data from various sources
def extract_data():
    """
    Extracts data from CSV files, Google Sheets, MySQL database, and HTML table.
    """
    # Download CSV files automatically
    download_file('https://assets.swisscoding.edu.vn/company_course/enrollies_education.xlsx', 'enrollies_education.csv')
    download_file('https://assets.swisscoding.edu.vn/company_course/work_experience.csv', 'work_experience.csv')

    # Extract Google Sheet data
    df_name = read_google_sheet('https://docs.google.com/spreadsheets/d/1VCkHwBjJGRJ21asd9pxW4_0z2PWuKhbLR3gUHm-p4GI/edit?usp=sharing')

    # Extract CSV data
    df_education = read_csv_file('enrollies_education.csv', 'Education Data')
    df_work_experience = read_csv_file('work_experience.csv', 'Work Experience Data')

    # Extract MySQL data
    connection_params = {
        "host": "112.213.86.31",
        "port": "3360",
        "user": "etl_practice",
        "password": "550814",
        "database": "company_course"
    }
    df_training_hours = read_mysql_table("SELECT * FROM training_hours", connection_params, "Training Hours Data")

    # Extract HTML table
    df_city_development_index = read_html_table('https://sca-programming-school.github.io/city_development_index/index.html', 'City Development Index')
    
    # Standardize column names in the HTML table
    df_city_development_index.columns = df_city_development_index.columns.str.lower()

    return df_name, df_education, df_work_experience, df_training_hours, df_city_development_index

# Transform Data (handle missing values, merge datasets)
def transform_data(df_name, df_education, df_work_experience, df_training_hours, df_city_development_index):
    """
    Cleans and merges the data.
    """
    # Fill missing gender values with 'Other'
    df_name['gender'].fillna('Other', inplace=True)
    
    # Merge the datasets on enrollee_id
    df_combined = df_name
    if df_education is not None:
        df_combined = pd.merge(df_combined, df_education, on='enrollee_id', how='left')
    if df_work_experience is not None:
        df_combined = pd.merge(df_combined, df_work_experience, on='enrollee_id', how='left')
    if df_training_hours is not None:
        df_combined = pd.merge(df_combined, df_training_hours, on='enrollee_id', how='left')
    if df_city_development_index is not None:
        df_combined = pd.merge(df_combined, df_city_development_index, on='city', how='left')

    return df_combined

# Load Data (for this example, we just print the result)
def load_data(df_combined):
    """
    Displays the combined DataFrame.
    """
    print("Combined Data - First 5 rows:")
    print(df_combined.head())

# Main ETL process
def main():
    """
    The main ETL function that orchestrates extraction, transformation, and loading of data.
    """
    # Step 1: Extract
    df_name, df_education, df_work_experience, df_training_hours, df_city_development_index = extract_data()

    # Step 2: Transform
    df_combined = transform_data(df_name, df_education, df_work_experience, df_training_hours, df_city_development_index)

    # Step 3: Load
    load_data(df_combined)

# Entry point of the script
if __name__ == "__main__":
    main()
