import matplotlib.pyplot as plt
import pandas as pd
import mysql.connector as dbconnect
from mysql.connector import Error
import config  # Import your MySQL credentials from config.py

# Function to connect to MySQL and fetch loan application data
def fetch_loan_application_data():
    try:
        # Connect to MySQL
        connection = dbconnect.connect(
            host='localhost',
            database='creditcard_capstone',
            user=config.user,
            password=config.password
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Query to fetch loan application data
            query = '''
                    SELECT *
                    FROM CDW_SAPP_loan_application
                    '''

            cursor.execute(query)
            records = cursor.fetchall()

            # Create a DataFrame for better manipulation
            df = pd.DataFrame(records, columns=[
                'Application_ID', 'Gender', 'Married', 'Dependents', 'Education', 
                'Self_Employed', 'Credit_History', 'Property_Area', 'Income', 'Application_Status'
            ])

            return df

    except Error as e:
        print(f"Error fetching data from MySQL: {e}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# Function to calculate approval percentage for self-employed applicants
def calculate_approval_percentage_self_employed(df):
    try:
        total_self_employed = df[df['Self_Employed'] == 'Yes'].shape[0]
        approved_self_employed = df[(df['Self_Employed'] == 'Yes') & (df['Application_Status'] == 'Y')].shape[0]

        if total_self_employed > 0:
            approval_percentage = (approved_self_employed / total_self_employed) * 100
        else:
            approval_percentage = 0.0

        return approval_percentage

    except Exception as e:
        print(f"Error calculating approval percentage for self-employed: {e}")
        return 0.0

# Function to calculate rejection percentage for married male applicants
def calculate_rejection_percentage_married_male(df):
    try:
        total_married_male = df[(df['Married'] == 'Yes') & (df['Gender'] == 'Male')].shape[0]
        rejected_married_male = df[(df['Married'] == 'Yes') & (df['Gender'] == 'Male') & (df['Application_Status'] == 'N')].shape[0]

        if total_married_male > 0:
            rejection_percentage = (rejected_married_male / total_married_male) * 100
        else:
            rejection_percentage = 0.0

        return rejection_percentage

    except Exception as e:
        print(f"Error calculating rejection percentage for married male applicants: {e}")
        return 0.0

# Function to create and save visualization for approval percentage of self-employed applicants
def create_and_save_approval_percentage_self_employed_plot(approval_percentage):
    plt.figure(figsize=(8, 6))

    labels = ['Approved', 'Rejected']
    sizes = [approval_percentage, 100 - approval_percentage]
    colors = ['#99ff99', '#ffcc99']

    plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=110)
    plt.title('Percentage of Approvals for Self-Employed Applicants')
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.tight_layout()

    # Save the plot as an image file
    plt.savefig('images/loan/approval_percentage_self_employed.png')

    # Display the plot (optional)
    plt.show()

# Function to create and save visualization for rejection percentage of married male applicants
def create_and_save_rejection_percentage_married_male_plot(rejection_percentage):
    plt.figure(figsize=(8, 6))

    labels = ['Rejected', 'Approved']
    sizes = [rejection_percentage, 100 - rejection_percentage]
    colors = ['#99ff99', '#ffcc99']

    plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=140)
    plt.title('Percentage of Rejections for Married Male Applicants')
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.tight_layout()

    # Save the plot as an image file
    plt.savefig('images/loan/rejection_percentage_married_male.png')

    # Display the plot (optional)
    plt.show()

# Function to connect to MySQL and fetch top three months with highest transaction volumes
def fetch_top_three_months_with_highest_transaction_volume():
    try:
        # Connect to MySQL
        connection = dbconnect.connect(
            host='localhost',
            database='creditcard_capstone',
            user=config.user,
            password=config.password
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Query to fetch top three months with highest transaction volumes
            query = '''
                    SELECT MONTH, COUNT(*) AS TRANSACTION_COUNT
                    FROM cdw_sapp_credit_card
                    GROUP BY MONTH
                    ORDER BY TRANSACTION_COUNT DESC
                    LIMIT 3
                    '''

            cursor.execute(query)
            records = cursor.fetchall()

            # Create a DataFrame for better manipulation
            df = pd.DataFrame(records, columns=['Month', 'Transaction Count'])

            # Convert numeric month values to month names
            month_names = {
                1: 'January', 2: 'February', 3: 'March', 4: 'April',
                5: 'May', 6: 'June', 7: 'July', 8: 'August',
                9: 'September', 10: 'October', 11: 'November', 12: 'December'
            }
            df['Month'] = df['Month'].map(month_names)

            return df  # Return DataFrame with top three months and their transaction counts

    except Error as e:
        print(f"Error fetching data from MySQL: {e}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# Function to create and save visualization for top three months with highest transaction volumes
def create_and_save_top_three_months_plot(df):
    color = plt.cm.Set2(1)  # Choose a single color from the Set2 colormap

    plt.figure(figsize=(6, 5))

    plt.bar(df['Month'], df['Transaction Count'], color=color, width=0.3)
    plt.xlabel('Month')
    plt.ylabel('Transaction Count')
    plt.title('Top Three Months with Highest Transaction Volumes')
    plt.ylim(3940, 3965)  # Set y-axis limits to reflect the range
    plt.tight_layout()

    # Save the plot as an image file
    plt.savefig('images/loan/top_three_months_transaction_volumes.png')

    # Display the plot (optional)
    plt.show()

# Function to connect to MySQL and fetch top three branches with highest total dollar value of healthcare transactions
def fetch_top_three_branches_healthcare_transactions():
    try:
        # Connect to MySQL
        connection = dbconnect.connect(
            host='localhost',
            database='creditcard_capstone',
            user=config.user,
            password=config.password
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Query to fetch top three branches with highest total dollar value of healthcare transactions
            query = '''
                    SELECT b.BRANCH_CITY AS Branch_City, b.BRANCH_STATE AS Branch_STATE,
                           ROUND(SUM(c.TRANSACTION_VALUE), 2) AS TOTAL_TRANSACTION_VALUE
                    FROM cdw_sapp_credit_card c
                    JOIN cdw_sapp_branch b ON c.BRANCH_CODE = b.BRANCH_CODE
                    WHERE c.TRANSACTION_TYPE = 'Healthcare'
                    GROUP BY b.BRANCH_CITY, b.BRANCH_STATE
                    ORDER BY TOTAL_TRANSACTION_VALUE DESC
                    LIMIT 3
                    '''

            cursor.execute(query)
            records = cursor.fetchall()

            # Create a DataFrame for better manipulation
            df = pd.DataFrame(records, columns=['Branch City', 'Branch State', 'Total Transaction Value'])

            return df  # Return DataFrame with top three branches and their total transaction values

    except Error as e:
        print(f"Error fetching data from MySQL: {e}")

    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

# Function to create and save visualization for top three branches with highest total dollar value of healthcare transactions
def create_and_save_top_three_branches_healthcare_plot(df):
    plt.figure(figsize=(10, 4))

    colors = ['#66c2a5', '#fc8d62', '#8da0cb'] 
    plt.barh(df['Branch City'] + ', ' + df['Branch State'], df['Total Transaction Value'], color=colors[:len(df)], height=0.3)

    # plt.barh(df['Branch Name'] + ', ' + df['Branch City'], df['Total Transaction Value'], color='skyblue', width=0.3)
    plt.xlabel('Total Transaction Value ($)')
    plt.ylabel('Branch')
    plt.title('Top Three Branches with Highest Total Dollar Value of Healthcare Transactions')
    plt.xlim(3900, 4400)  # Set x-axis limits to reflect the range
    plt.tight_layout()

    # Save the plot as an image file
    plt.savefig('images/loan/top_three_branches_healthcare_transactions.png')

    # Display the plot (optional)
    plt.show()

# Main function to orchestrate the loan module process with menu selection
def main():
    while True:
        print("\nLoan Data Analysis Menu:")
        print("1. Percentage of Approvals for Self-Employed Applicants")
        print("2. Percentage of Rejections for Married Male Applicants")
        print("3. Calculate and Plot Top Three Months with Highest Transaction Volumes")
        print("4. Top Three Branches with Highest Total Dollar Value of Healthcare Transactions")
        print("5. Exit")

        choice = input("Enter your choice (1-5): ")

        if choice == '1':
            # Fetch loan application data from MySQL
            loan_data = fetch_loan_application_data()

            if loan_data is not None:
                # Calculate approval percentage for self-employed applicants
                approval_percentage = calculate_approval_percentage_self_employed(loan_data)

                # Print the calculated percentage
                print(f"Percentage of approvals for self-employed applicants: {approval_percentage:.2f}%")

                # Create and save the visualization
                create_and_save_approval_percentage_self_employed_plot(approval_percentage)

            else:
                print("No loan application data fetched from the database.")

        elif choice == '2':
            # Fetch loan application data from MySQL
            loan_data = fetch_loan_application_data()

            if loan_data is not None:
                # Calculate rejection percentage for married male applicants
                rejection_percentage = calculate_rejection_percentage_married_male(loan_data)

                # Print the calculated percentage
                print(f"Percentage of rejections for married male applicants: {rejection_percentage:.2f}%")

                # Create and save the visualization
                create_and_save_rejection_percentage_married_male_plot(rejection_percentage)

            else:
                print("No loan application data fetched from the database.")
        
        elif choice == '3':
            # Fetch top three months with highest transaction volumes
            df = fetch_top_three_months_with_highest_transaction_volume()

            if df is not None:
                # Display the fetched data
                print("Top Three Months with Highest Transaction Volumes:")
                print(df.to_string(index=False))

                # Create and save the plot
                create_and_save_top_three_months_plot(df)
            else:
                print("No data fetched from the database.")

        elif choice == '4':
            # Fetch top three branches with highest total dollar value of healthcare transactions
            df = fetch_top_three_branches_healthcare_transactions()

            if df is not None:
                # Display the fetched data
                print("Top Three Branches with Highest Total Dollar Value of Healthcare Transactions:")
                print(df.to_string(index=False))

                # Create and save the plot
                create_and_save_top_three_branches_healthcare_plot(df)
            else:
                print("No data fetched from the database.")

        elif choice == '5':
            print("Exiting...")
            break

        else:
            print("Invalid choice. Please enter 1, 2, 3, 4, or 5.")

if __name__ == "__main__":
    main()
