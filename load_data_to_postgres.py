import os
import pandas as pd
from sqlalchemy import create_engine, types

# Retrieve the PostgreSQL password from an environment variable
db_password = os.getenv('DB_PASSWORD', 'yourpassword')  

# PostgreSQL database connection string with password hidden
conn_string = f'postgresql://postgres:{db_password}@localhost/tr_electricity'
db = create_engine(conn_string)
conn = db.connect()

# Directory path where the CSV files are located
directory_path = '/Users/Analiz/Documents/Project_trm/Tr_electricity/Hourly_gen/Data'

# List all CSV files in the directory
csv_files = [os.path.join(directory_path, file) for file in os.listdir(directory_path) if file.endswith('.csv')]

# Read each CSV file into a DataFrame and concatenate them into a single DataFrame
dfs = [pd.read_csv(file, delimiter=';') for file in csv_files]
df = pd.concat(dfs, ignore_index=True)

# Convert column names to lowercase and replace spaces and hyphens with underscores
df.columns = [col.lower().replace(' ', '_').replace('-', '_') for col in df.columns]

# Convert 'date' and 'hour' columns to appropriate data types
df['date'] = pd.to_datetime(df['date'], format='%d.%m.%Y')
df['hour'] = pd.to_datetime(df['hour'], format='%H:%M').dt.time

# Define data types for each column
column_data_types = {
    'date': types.Date, 
    'hour': types.Time, 
    **{col: types.Float for col in df.columns if col not in ['date', 'hour']}
}

# Write the DataFrame to a PostgreSQL table
# If the table exists, replace it with the new data
df.to_sql('hourly_gen', con=conn, if_exists='replace', index=False, dtype=column_data_types)

# Close the database connection
conn.close()
