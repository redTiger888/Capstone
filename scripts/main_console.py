import os
import subprocess

def run_etl_cc_data():
    print()
    print("Running ETL process...")
    subprocess.run(["python", "scripts/etl_credit_card_data.py"])

def run_etl_loan_data():
    print()
    print("Fetching loan data...")
    subprocess.run(["python", "scripts/etl_loan_data.py"])

def view_zipcode_transactions():
    print()
    print("Running Transaction Module...")
    subprocess.run(["python", "scripts/Transaction_module.py"])

def view_customer_details():
    print()
    print("Running Customer Module...")
    subprocess.run(["python", "scripts/customer_module.py"])

def analyze_credit_card_transactions():
    print()
    print("Running Credit Card Transaction Data Analysis...")
    subprocess.run(["python", "scripts/credit_card_data_analysis.py"])

def analyze_loan_applications():
    print()
    print("Running Loan Application Data Analysis...")
    subprocess.run(["python", "scripts/loan_data_analysis.py"])

def load_database():
    print("Loading database...")
    run_etl_cc_data()
    run_etl_loan_data()
    
def exit_system():
    print("Goodbye!")
    exit()

def main():
    print("Welcome to the Credit Card System!")
    print("Initializing system...")
    
    while True:
        print("\nCredit Card System Menu:")
        print("1. View ALL Monthly transactions for a Zip Code")
        print("2. Customer Details Module")
        print("3. Credit Card Transaction Data Analysis")
        print("4. Loan Application Data Analysis")
        print("5. Load Data - Warning will erase all data (Credit Card data & Loan data) and reload database")
        print("6. Exit")

        choice = input("Enter your choice (1-6): ")

        if choice == '1':
            view_zipcode_transactions()
        elif choice == '2':
            view_customer_details()
        elif choice == '3':
            analyze_credit_card_transactions()
        elif choice == '4':
            analyze_loan_applications()
        elif choice == '5':
            load_database()
        elif choice == '6':
            exit_system()
        else:
            print("Invalid choice. Please enter a number between 1 and 6.")

if __name__ == "__main__":
    main()
