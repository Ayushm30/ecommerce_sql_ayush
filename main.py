import pandas as pd
import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Database connection setup using pymysql
DATABASE_URI = "mysql+pymysql://root:12345678@localhost/ecommerce"
engine = create_engine(DATABASE_URI)

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('order_item.csv', 'order_item'),
    ('payments.csv', 'payments')
]

# Folder containing the CSV files
folder_path = '/Users/apple/Desktop/ecommerce_sql/archive'


# Function to infer SQL data types from Pandas data types
def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'


# Process each CSV file
for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)

    try:
        # Read the CSV file into a Pandas DataFrame
        df = pd.read_csv(file_path)

        # Replace NaN values with None to handle SQL NULL
        df = df.where(pd.notnull(df), None)

        # Debugging: Check for NaN values
        print(f"Processing {csv_file}")
        print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

        # Clean column names for SQL compatibility
        df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

        # Generate CREATE TABLE statement dynamically
        columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
        create_table_query = text(f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})')

        # Open a connection to MySQL and handle rollback properly
        with engine.connect() as connection:
            transaction = connection.begin()
            try:
                # Create the table if it doesn't exist
                connection.execute(create_table_query)

                # Insert DataFrame data into MySQL
                df.to_sql(table_name, con=engine, if_exists='append', index=False, method='multi')

                # Commit transaction
                transaction.commit()
                print(f"✅ Successfully imported {csv_file} into `{table_name}`\n")

            except Exception as e:
                transaction.rollback()
                print(f"❌ Rolled back due to error in {csv_file}: {e}")

    except SQLAlchemyError as e:
        print(f"❌ Database error in {csv_file}: {e}")

    except Exception as e:
        print(f"Unexpected error: {e}")

print("🎉 All CSV files have been successfully imported into MySQL!")
