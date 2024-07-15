import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector
from mysql.connector import Error
import config  # Import your MySQL credentials from config.py

# Function to connect to MySQL and fetch transaction data
def fetch_highest_transaction_count():
    try:
        # Connect to MySQL
        connection = mysql.connector.connect(
            host='localhost',
            database='creditcard_capstone',
            user=config.user,
            password=config.password
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Query to fetch transaction type and count
            query = '''
                    SELECT TRANSACTION_TYPE, COUNT(*) AS TRANSACTION_COUNT
                    FROM cdw_sapp_credit_card
                    GROUP BY TRANSACTION_TYPE
                    '''

            cursor.execute(query)
            records = cursor.fetchall()

            # Create a DataFrame for better manipulation
            df = pd.DataFrame(records, columns=['TRANSACTION_TYPE', 'TRANSACTION_COUNT'])

            # Sort DataFrame in descending order for display
            df_sorted = df.sort_values(by='TRANSACTION_COUNT', ascending=False)

            return df_sorted, df  # Return both sorted and unsorted DataFrames

    except Error as e:
        print(f"Error fetching data from MySQL: {e}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# Function to create and save visualization
def create_and_save_plot(df):
    plt.figure(figsize=(10, 6))

    # Plotting the unsorted DataFrame for visualization
    plt.bar(df['TRANSACTION_TYPE'], df['TRANSACTION_COUNT'])
    plt.xlabel('Transaction Type')
    plt.ylabel('Transaction Count')
    plt.title('Transaction Count by Transaction Type')
    plt.xticks(rotation=45)
    plt.ylim(6500, 6900)  # Set y-axis limits to reflect the range
    plt.tight_layout()

    # Save the plot as an image file
    plt.savefig('transaction_count_by_type.png')

    # Display the plot (optional)
    plt.show()

    # # Print the sorted DataFrame
    # print("\nSorted DataFrame:")
    # print(df_sorted)

# Main function to orchestrate the process
def main():
    # Fetch transaction data
    df_sorted, df = fetch_highest_transaction_count()

    if df_sorted is not None:
        # Display the fetched data (optional)
        print("Fetched Highest Transaction Count Data:")
        print(df_sorted)

        # Create and save the plot
        create_and_save_plot(df)
    else:
        print("No data fetched from the database.")

if __name__ == "__main__":
    main()
