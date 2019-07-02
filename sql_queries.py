import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES


amount_table_drop = "DROP TABLE IF EXISTS amount;"
country_table_drop = "DROP TABLE IF EXISTS country;"
loan_status_table_drop = "DROP TABLE IF EXISTS loan_status;"
loan_type_table_drop = "DROP TABLE IF EXISTS loan_type;"
transaction_table_drop = "DROP TABLE IF EXISTS transaction;"
time_table_drop = "DROP TABLE IF EXISTS time;"
staging_time_table_drop = "DROP TABLE IF EXISTS staging_time;"
staging_transaction_table_drop = "DROP TABLE IF EXISTS staging_transaction;"

# CREATE TABLES

amount_table_create= ("""create table if not exists amount (
Amount_Id INTEGER PRIMARY KEY,
Original_Principal_Amount float, 
Sold_3rd_Party float, 
Repaid_3rd_Party float, 
Due_3rd_Party float
);
""")

country_table_create = ("""create table if not exists country (
Country_Code VARCHAR(50),
Country VARCHAR(50),
Region VARCHAR(50),
Country_Id INTEGER PRIMARY KEY
);
""")

loan_status_table_create = ("""create table if not exists loan_status (
Loan_Status VARCHAR(50),
Loan_Status_Id INTEGER PRIMARY KEY
);
""")

loan_type_table_create = ("""create table if not exists loan_type (
Loan_Type VARCHAR(50),
Loan_Type_Id INTEGER PRIMARY KEY 
);
""")

transaction_table_create = ("""create table if not exists transaction (
Loan_Number VARCHAR(50) PRIMARY KEY, 
Time_Id integer, 
Country_Id integer, 
Guarantor_Country_Id INTEGER, 
Loan_Type VARCHAR(50),
Loan_Status_Id integer,
Amount_Id integer,
End_of_Period timestamp,
Interest_Rate float,
Project_ID VARCHAR(50),
Exchange_Adjustment float,
Borrowers_Obligation float,
Cancelled_Amount float,
Undisbursed_Amount float,
Disbursed_Amount float,
Repaid_to_IBRD float,
Due_to_IBRD float,
Loans_Held float
);
""")

staging_transaction_table_create = ("""create table if not exists staging_transaction (
Loan_Number VARCHAR(50) PRIMARY KEY, 
Time_Id integer, 
Country_Id integer, 
Guarantor_Country_Id INTEGER, 
Loan_Type VARCHAR(50),
Loan_Status_Id integer,
Amount_Id integer,
End_of_Period VARCHAR(50),
Interest_Rate float,
Project_ID VARCHAR(50),
Exchange_Adjustment float,
Borrowers_Obligation float,
Cancelled_Amount float,
Undisbursed_Amount float,
Disbursed_Amount float,
Repaid_to_IBRD float,
Due_to_IBRD float,
Loans_Held float
);
""")

staging_time_table_create = ("""create table if not exists staging_time (
Time_Id INTEGER PRIMARY KEY, 
First_Repayment_Date VARCHAR(50),
Last_Repayment_Date VARCHAR(50),
Agreement_Signing_Date VARCHAR(50),
Board_Approval_Date VARCHAR(50),
Effective_Date_Most_Recent VARCHAR(50),
Closed_Date_Most_Recent VARCHAR(50)
);
""")

time_table_create = ("""create table if not exists time (
Time_Id INTEGER PRIMARY KEY, 
First_Repayment_Date TIMESTAMP,
Last_Repayment_Date TIMESTAMP,
Agreement_Signing_Date TIMESTAMP,
Board_Approval_Date TIMESTAMP,
Effective_Date_Most_Recent TIMESTAMP,
Closed_Date_Most_Recent TIMESTAMP
);
""")

# STAGING TABLES

loan_type_copy = ("""copy loan_type from {}
credentials 'aws_iam_role={}'
CSV;
""").format(config.get('S3','Loan_type_DATA'), config.get('IAM_ROLE','ARN'))

staging_transaction_copy = ("""copy staging_transaction from {}
credentials 'aws_iam_role={}'
CSV;
""").format(config.get('S3','Transaction_DATA'), config.get('IAM_ROLE','ARN'))

country_copy = ("""copy country from {}
credentials 'aws_iam_role={}'
CSV;
""").format(config.get('S3','Country_DATA'), config.get('IAM_ROLE','ARN'))

loan_status_copy = ("""copy loan_status from {}
credentials 'aws_iam_role={}'
CSV;
""").format(config.get('S3','Loan_status_DATA'), config.get('IAM_ROLE','ARN'))

staging_time_copy = ("""copy staging_time from {}
credentials 'aws_iam_role={}'
CSV;
""").format(config.get('S3','Time_DATA'), config.get('IAM_ROLE','ARN'))

amount_copy = ("""copy amount from {}
credentials 'aws_iam_role={}'
CSV;
""").format(config.get('S3','Amount_DATA'), config.get('IAM_ROLE','ARN'))

# INSERT TABLES

time_table_insert = ("""insert into time select Time_Id,
to_timestamp(first_repayment_Date,'YYYYMMDD'), to_timestamp(Last_Repayment_Date,'YYYYMMDD'),to_timestamp(Agreement_Signing_Date,'YYYYMMDD'),
to_timestamp(Board_Approval_Date,'YYYYMMDD'), to_timestamp(Effective_Date_Most_Recent,'YYYYMMDD'), to_timestamp(Closed_Date_Most_Recent,'YYYYMMDD')
from staging_time;
""")

transaction_table_insert = ("""insert into transaction select Loan_Number,
Time_Id, Country_Id, Guarantor_Country_Id, Loan_Type, Loan_Status_Id, Amount_Id, to_timestamp(End_of_Period,'YYYYMMDD'), Interest_Rate, Project_ID,
Exchange_Adjustment, Borrowers_Obligation, Cancelled_Amount, Undisbursed_Amount, Disbursed_Amount, Repaid_to_IBRD, Due_to_IBRD,Loans_Held
from staging_transaction;
""")

# QUERY LISTS

create_table_queries = [amount_table_create, country_table_create, loan_status_table_create, loan_type_table_create, transaction_table_create, time_table_create, staging_time_table_create, staging_transaction_table_create]
drop_table_queries = [amount_table_drop, country_table_drop, loan_status_table_drop, loan_type_table_drop, transaction_table_drop, time_table_drop, staging_time_table_drop, staging_transaction_table_drop]
copy_table_queries = [staging_transaction_copy, loan_type_copy, country_copy, loan_status_copy, staging_time_copy, amount_copy]
insert_table_queries = [time_table_insert, transaction_table_insert]
drop_staging_table_queries = [staging_time_table_drop,staging_transaction_table_drop]
