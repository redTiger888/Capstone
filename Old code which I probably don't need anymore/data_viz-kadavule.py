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

# Function to create and save visualization for highest transaction count by type
def create_and_save_plot(df):
    plt.figure(figsize=(10, 6))

    plt.bar(df['TRANSACTION_TYPE'], df['TRANSACTION_COUNT'])
    plt.xlabel('Transaction Type')
    plt.ylabel('Transaction Count')
    plt.title('Transaction Count by Transaction Type')
    plt.xticks(rotation=45)
    plt.ylim(6500, 6900)  # Set y-axis limits to reflect the range
    plt.tight_layout()

    # Save the plot as an image file
    plt.savefig('highest_transaction_count_by_type.png')

    # Display the plot (optional)
    plt.show()

# Function to connect to MySQL and fetch top 10 states with highest customers
def fetch_top_10_states_with_highest_customers():
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

            # Query to fetch top 10 states with highest number of customers
            query = '''
                    SELECT CUST_STATE, COUNT(*) AS CUSTOMER_COUNT
                    FROM cdw_sapp_customer
                    GROUP BY CUST_STATE
                    ORDER BY CUSTOMER_COUNT DESC
                    LIMIT 10
                    '''

            cursor.execute(query)
            records = cursor.fetchall()

            # Create a DataFrame for better manipulation
            df = pd.DataFrame(records, columns=['CUST_STATE', 'CUSTOMER_COUNT'])

            # Sort DataFrame by customer count for printing
            df_sorted = df.sort_values(by='CUSTOMER_COUNT', ascending=False)

            return df_sorted, df  # Return both sorted and unsorted DataFrames

    except Error as e:
        print(f"Error fetching data from MySQL: {e}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# Function to create and save visualization for top 10 states with highest customers
def create_and_save_top_10_states_plot(df):
    plt.figure(figsize=(10, 6))

    # Sort DataFrame by state alphabetically for plotting
    df_sorted_plot = df.sort_values(by='CUST_STATE')

    plt.bar(df_sorted_plot['CUST_STATE'], df_sorted_plot['CUSTOMER_COUNT'])
    plt.xlabel('Customer State')
    plt.ylabel('Number of Customers')
    plt.title('Top 10 States with Highest Number of Customers (Sorted by State)')
    plt.xticks(rotation=45)
    plt.tight_layout()

    # Save the plot as an image file
    plt.savefig('top_10_states_customers.png')

    # Display the plot (optional)
    plt.show()

# Function to display console menu and orchestrate the process
def display_menu():
    print("Welcome to the Data Visualization Menu!")
    print("1. Highest Transaction Count by Type")
    print("2. Top 10 States with Highest Number of Customers")
    print("3. Exit")

    choice = input("Please enter your choice (1/2/3): ")

    if choice == '1':
        # Fetch and visualize highest transaction count by type
        df_sorted, df = fetch_highest_transaction_count()
        if df_sorted is not None:
            print("Fetched Highest Transaction Count Data:")
            print(df_sorted.to_string(index=False))
            create_and_save_plot(df)
        else:
            print("No data fetched from the database.")

    elif choice == '2':
        # Fetch and visualize top 10 states with highest number of customers
        df_sorted, df = fetch_top_10_states_with_highest_customers()
        if df_sorted is not None:
            print("Sorted DataFrame (Top 10 States with Highest Number of Customers):")
            print(df_sorted.to_string(index=False))
            create_and_save_top_10_states_plot(df)
        else:
            print("No data fetched from the database.")

    elif choice == '3':
        print("Exiting the program. Goodbye!")

    else:
        print("Invalid choice. Please enter 1, 2, or 3.")
        display_menu()

if __name__ == "__main__":
    display_menu()
