# This file reads the different data files and created a DataFrame
from lib import ConfigReader

# This function reads the initial raw data file and returns a DataFrame.
def read_initial_raw_data(spark,env):
    conf=ConfigReader.get_app_config(env)
    initial_raw_data_file_path=conf['initial_raw_data.file.path']
    initial_raw_data_df=spark.read.csv(initial_raw_data_file_path,inferSchema=True, header=True)
    return initial_raw_data_df

#This function reads the raw customer data from a CSV file and applies the defined schema.
def read_raw_customer_data(spark, env):
    conf=ConfigReader.get_app_config(env)
    raw_customer_file_path=conf['customers_raw.file.path']
    schema="member_id string, emp_title	string, emp_length string, home_ownership string, annual_income float, address_state string, address_zip_code string, address_country string, grade string, sub_grade string, verification_status string, total_high_credit_lim float, application_type string, joint_annual_income float, verification_status_joint string"
    raw_customer_df= spark.read.csv(raw_customer_file_path, schema=schema, header=True)
    return raw_customer_df

def read_raw_loan_data(spark,env):
    conf=ConfigReader.get_app_config(env)
    raw_loan_file_path=conf['loans_raw.file.path']
    schema="loan_id string, member_id string, loan_amount float, funded_amount float, loan_term string, interest_rate float, monthly_installment float,issue_date string, loan_status string, loan_purpose string, loan_title string"
    raw_loan_df=spark.read.csv(raw_loan_file_path, schema=schema, header=True)
    return raw_loan_df

def read_raw_loan_repayment_data(spark,env):
    conf=ConfigReader.get_app_config(env)
    raw_loan_repayment_file_path=conf['loan_repayment_raw.file.path']
    schema="loan_id string, total_principal_received float, total_interest_received float, total_late_fee_received float, total_payment_received float, last_payment_amount float, last_payment_date string, next_payment_date string"
    raw_loan_repayment_df=spark.read.csv(raw_loan_repayment_file_path,  schema=schema, header=True)
    return raw_loan_repayment_df

def read_raw_loan_defaulter_data(spark, env):
    conf=ConfigReader.get_app_config(env)
    raw_loan_defaulter_file_path=conf['loan_defaulter_raw.file.path']
    loan_defaulters_schema = "member_id string, delinq_2yrs float, delinq_amnt float, pub_rec float, pub_rec_bankruptcies float,inq_last_6mths float, total_rec_late_fee float, mths_since_last_delinq float, mths_since_last_record float"
    raw_loan_defaulter_df=spark.read.csv(raw_loan_defaulter_file_path, header=True,schema=loan_defaulters_schema)
    return raw_loan_defaulter_df