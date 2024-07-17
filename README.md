# ETL and Data Analysis for Loan and Credit Card Datasets
## Poongodi Velayutham
### Per Scholas Data Engineering Capstone
### 7-15-2024

### Project Overview
This Capstone Project demonstrates proficiency in managing an ETL (Extract, Transform, Load) process for two datasets: Loan Application (API) and Credit Card (JSON).
I've used Python (Pandas, Matplotlib), MySQL, Apache Spark (Spark Core, Spark SQL), and Python Visualization and Analytics libraries.

#### Workflow Diagram of Requirements
![Screenshot 2024-07-16 125119](https://github.com/user-attachments/assets/e9648525-e0d0-4947-af82-90b5ad2a4623)

### Technologies Used
- **Python**: Utilizing Pandas for data manipulation, Matplotlib for visualizations, MySQL connectors
- **MYSQL**: Managing database operations, schema design, and data loading.
- **Apache Spark**: Employing Spark Core and Spark SQL for distributed data processing.
- **Python Visualization and Analytics Libraries**: Generating visual insights from the datasets.

### Prerequisites
- Python 3.11 
- MySQL server
- Required Python packages (listed in requirements.txt)

### Project File Structure
- **docs**:
  - **CAP 350 - Data Engineering - Capstone Project Requirements Document.pdf**: Contains detailed project requirements and specifications.
  - **Mapping Document.xlsx**: Provides mapping and transformation rules for data processing.
- **data**:
  - **cdw_sapp_branch.json**: JSON file containing branch data.
  - **cdw_sapp_credit.json**: JSON file containing credit card data.
  - **cdw_sapp_customer.json**: JSON file containing customer data.
- **scripts**:
  - **customer_module.py**: Handles customer details operations.
  - **etl_credit_card_data.py**: Performs ETL for the Credit Card dataset.
  - **etl_loan_data.py**: Fetches and processes data from the Loan Application API.
  - **loan_data_analysis.py**: Analyzes and visualizes data from the Loan Application dataset.
  - **main_console.py**: Console-based menu for interacting with the database and running analyses.
  - **credit_card_data_analysis.py**: Analyzes and visualizes Credit Card transaction data.
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
  **Req 1.1:** 
  #### ETL: etl_credit_card_data.py
  - **Extract data from JSON files using Python and PySpark:**
    - credit_card_data.json
    - credit_card_transactions.json
    - credit_card_customers.json
  - **Additional Transform decisions not explicitly stated in requirements:**
    - **cdw_sapp_branch.json**
      - Implemented specific transformations according to mapping document requirements.
      - Added a leading zero to 4-digit zip codes to ensure they are 5 digits long.
    - **credit_card_customers.json:**
      - Handled missing area codes by setting them to '000' where not provided, recognizing that users' cell phone numbers might not correspond to their geographic locations.
 #### **Req 1.2:** 
  - **Loading data:**
      **Database Operations:**
      - Created a MySQL database (`creditcard_capstone`).
      - Load transformed data into respective tables (`CDW_SAPP_BRANCH`, `CDW_SAPP_CREDIT_CARD`, `CDW_SAPP_CUSTOMER`).
#### **Req 2**
#### main_console.py
![Screenshot 2024-07-16 192125](https://github.com/user-attachments/assets/b89c63d4-fa07-4a55-ab7a-4ebd0ea6d014)

##### **Req 2.1** **transaction_module.py**
##### **Req 2.2** **customer_module.py**
![Screenshot 2024-07-16 192359](https://github.com/user-attachments/assets/7e8865c2-d033-4f70-bceb-aa9249a814bf)
![Screenshot 2024-07-16 192448](https://github.com/user-attachments/assets/bf8eefce-a2ba-4d87-9508-e260e9d07f6b)
![Screenshot 2024-07-16 192623](https://github.com/user-attachments/assets/b85b1a16-7828-403d-a1e1-00af31806c2a)

##### **Req 3** **credit_card_data_analysis.py**
**Credit Card Data Analysis and Visualization:**
- Utilize Python libraries to analyze and visualize data insights:
  - Transaction types with the highest counts.
  - Top states with the highest number of customers.
  - Top customers based on transaction amounts.
![Screenshot 2024-07-16 192448](https://github.com/user-attachments/assets/95743579-6c65-47a3-b531-a5acb5e2e938)
![Screenshot 2024-07-16 194346](https://github.com/user-attachments/assets/4397a82a-73c9-475e-b950-fd9aa3d0c6d6)
![Screenshot 2024-07-16 194752](https://github.com/user-attachments/assets/a12ba4b7-5654-49cd-98c5-4c5d0dfb53ce)
![Screenshot 2024-07-16 195601](https://github.com/user-attachments/assets/dc46695c-8181-4d70-ad59-7f71477b7ee9)

##### **Req 4** **etl_loan_data.py**
**API Integration (Loan Application):**
- Fetch data from the Loan Application API endpoint.
- Utilize PySpark to load API data into the `CDW_SAPP_loan_application` table in the MySQL database.
  
##### **Req 5** **loan_data_analysis.py**
![Screenshot 2024-07-16 200025](https://github.com/user-attachments/assets/a087f7c3-20a0-4d14-95a0-502bff652696)
![Screenshot 2024-07-16 200105](https://github.com/user-attachments/assets/52f5b564-9e88-4cae-8896-261daf8d8715)
![Screenshot 2024-07-16 201309](https://github.com/user-attachments/assets/5f6e7b52-530e-49f9-954d-c4583c9cadd7)
![Screenshot 2024-07-16 202422](https://github.com/user-attachments/assets/731d8833-6f1b-4c9e-8cde-93f77fecc6ef)
![Screenshot 2024-07-16 203546](https://github.com/user-attachments/assets/707fd0f3-68fe-4f95-9674-d183868979bb)

### Running the Project
To run the project, execute `main_console.py` and follow the menu options to interact with different functionalities.
