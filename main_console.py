# main_console.py

import os
import subprocess

def run_etl():
    print("Running ETL process...")
    subprocess.run(["python", "etl.py"])

def run_get_loan_data():
    print("Fetching loan data...")
    subprocess.run(["python", "get_loan_data.py"])

def view_zipcode_transactions():
    print("Running Transaction Module...")
    subprocess.run(["python", "Transaction_module.py"])

def view_customer_details():
    print("Running Customer Module...")
    subprocess.run(["python", "customer_module.py"])

def analyze_credit_card_transactions():
    print("Running Credit Card Transaction Data Analysis...")
    subprocess.run(["python", "transaction_data_analysis.py"])

def analyze_loan_applications():
    print("Running Loan Application Data Analysis...")
    subprocess.run(["python", "loan_data_analysis.py"])

def exit_system():
    print("Goodbye!")
    exit()

def main():
    print("Welcome to the Credit Card System!")
    print("Initializing system...")
    run_etl()
    run_get_loan_data()
    
    while True:
        print("\nCredit Card System Menu:")
        print("1. View a Zip code's Monthly transactions")
        print("2. Customer Details Module")
        print("3. Credit Card Transaction Data Analysis")
        print("4. Loan Application Data Analysis")
        print("5. Exit")

        choice = input("Enter your choice (1-5): ")

        if choice == '1':
            view_zipcode_transactions()
        elif choice == '2':
            view_customer_details()
        elif choice == '3':
            analyze_credit_card_transactions()
        elif choice == '4':
            analyze_loan_applications()
        elif choice == '5':
            exit_system()
        else:
            print("Invalid choice. Please enter a number between 1 and 5.")

if __name__ == "__main__":

    main()
