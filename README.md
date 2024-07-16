# ETL and Data Analysis for Loan and Credit Card Datasets
## Poongodi Velayutham
### Per Scholas Data Engineering Capstone
### 7-15-2024

### Project Overview
This Capstone Project demonstrates proficiency in managing an ETL (Extract, Transform, Load) process for two datasets: Loan Application (API) and Credit Card (JSON).
I've used Python (Pandas, Matplotlib), MySQL, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries.

### Technologies Used
- **Python**: Utilizing Pandas for data manipulation, Matplotlib for visualizations, MySQL connectors
- **MYSQL**: Managing database operations, schema design, and data loading.
- **Apache Spark**: Employing Spark Core and Spark SQL for distributed data processing.
- **Python Visualization and Analytics Libraries**: Generating visual insights from the datasets.

### Prerequisites
- Python 3.11 
- MySQL server
- Required Python packages (listed in requirements.txt)

### Project Structure and Files
- **docs**:
  - **CAP 350 - Data Engineering - Capstone Project Requirements Document.pdf**: Contains detailed project requirements and specifications.
  - **Mapping Document.xlsx**: Provides mapping and transformation rules for data processing.
- **scripts**:
  - **customer_module.py**: Handles customer details operations.
  - **etl_credit_card_data.py**: Performs ETL for the Credit Card dataset.
  - **etl_loan_data.py**: Fetches and processes data from the Loan Application API.
  - **loan_data_analysis.py**: Analyzes and visualizes data from the Loan Application dataset.
  - **main_console.py**: Console-based menu for interacting with the database and running analyses.
  - **transaction_data_analysis.py**: Analyzes and visualizes Credit Card transaction data.
  - **transaction_module.py**: Module for transaction details operations.
- **logs**:
  - **customer_module.log**: Log file for customer module operations.
  - **etl_credit_card.log**: Log file for ETL operations on the Credit Card dataset.
  - **etl_loan_data.log**: Log file for fetching and processing Loan Application API data.
  - **transaction_module.log**: Log file for transaction module operations.
- **images**:
  - **credit**:
    - **highest_transaction_count_by_type.png**: Visualizes transaction counts by type for Credit Card data.
    - **top_10_customers_transaction_sums.png**: Displays transaction sums for the top 10 customers.
    - **top_10_states_customers.png**: Shows customer counts across the top 10 states.
  - **loan**:
    - **approval_percentage_self_employed.png**: Illustrates approval percentages for self-employed applicants in loan data.
    - **rejection_percentage_married_male.png**: Highlights rejection percentages for married male applicants.
    - **top_three_months_transaction_volumes.png**: Displays transaction volumes for the top three months.


### Project Goals
**Data Extraction and Transformation:**
- Extract data from JSON files (Loan and Credit Card datasets) using Python and PySpark.
- Transform data based on requirements specified in the mapping document.

**Database Operations:**
- Create and manage a MySQL database (`creditcard_capstone`).
- Load transformed data into respective tables (`CDW_SAPP_BRANCH`, `CDW_SAPP_CREDIT_CARD`, `CDW_SAPP_CUSTOMER`).

**Front-End Application:**
- Develop a console-based menu (Python program) to interact with the database.
- Implement functionalities to display transaction details, customer information, and generate bills.

**Data Analysis and Visualization:**
- Utilize Python libraries to analyze and visualize data insights:
  - Transaction types with the highest counts.
  - Top states with the highest number of customers.
  - Top customers based on transaction amounts.
  - Other specified visualizations like loan approval rates and transaction volumes.

**API Integration (Loan Application):**
- Fetch data from the Loan Application API endpoint.
- Utilize PySpark to load API data into the `CDW_SAPP_loan_application` table in the MySQL database.

### Running the Project
To run the project, execute `main_console.py` and follow the menu options to interact with different functionalities.
