import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector
from mysql.connector import Error
import config  # Import your MySQL credentials from config.py

# Function to connect to MySQL and fetch transaction data
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

# Function to create and save visualization
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

# Main function to orchestrate the process
def main():
    # Fetch top 10 states with highest number of customers
    df_sorted, df = fetch_top_10_states_with_highest_customers()

    if df_sorted is not None:
        # Display the fetched data (optional)
        print("Sorted DataFrame (Top 10 States with Highest Number of Customers):")
        print(df_sorted.to_string(index=False))

        # Create and save the plot using the unsorted DataFrame
        create_and_save_top_10_states_plot(df)
    else:
        print("No data fetched from the database.")

if __name__ == "__main__":
    main()
