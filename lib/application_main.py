import sys
from lib import DataReader, utils,DataManipulation
from pyspark.sql.functions import *

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print("Please specify the environment")
		sys.exit(-1)

	job_run_env = sys.argv[1]
	print("Creating Spark Session")

	spark = utils.get_spark_session(job_run_env)
	spark.sparkContext.setLogLevel("ERROR")
	print("Created Spark Session")

	"""
	initial_raw_data_df = DataReader.read_initial_raw_data(spark, job_run_env)
	print("Read initial raw data")
	
	print("Generating key for initial raw data")
	initial_raw_data_df_with_key = DataManipulation.generate_key(initial_raw_data_df)
    
    """
	read_raw_customer_df = DataReader.read_raw_customer_data(spark, job_run_env)
	print("Read raw customer data")

	add_ingestion_timestamp_customer = DataManipulation.add_ingestion_timestamp_customer(read_raw_customer_df)
	print("Added ingestion timestamp to customer data")

	remove_duplicates_customer = DataManipulation.remove_duplicates_customer(add_ingestion_timestamp_customer)
	print("Removed duplicates from customer data")

	remove_null_annual_income = DataManipulation.remove_null_annual_income(remove_duplicates_customer)
	print("Removed null annual income from customer data")

	convert_emp_length_to_int = DataManipulation.convert_emp_length_to_int(remove_null_annual_income)
	print("Converted employee length to int in customer data")

	null_with_avg_emp_length = DataManipulation.null_with_avg_emp_length(convert_emp_length_to_int)
	print("Filled null employee length with average in customer data")

	clean_customer_data = DataManipulation.clean_customer_address(null_with_avg_emp_length)
	print("Cleaned customer address data")

	DataManipulation.write_cleaned_customer_data(clean_customer_data)
	print("Wrote cleaned customer data to CSV")

	"""
	read_loans_df = DataReader.read_raw_loan_data(spark, job_run_env)
	print("Read raw loans data")	

	add_ingestion_timestamp_loan = DataManipulation.add_ingestion_timestamp_loan(read_loans_df)
	print("Added ingestion timestamp to loan data")

	loans_df_remove_null = DataManipulation.loans_df_remove_nulls(add_ingestion_timestamp_loan)
	print("Removed null values from loan data")

	convert_loan_term=DataManipulation.convert_loan_term(loans_df_remove_null)
	print("Converted loan term to int in loan data")

	loans_cleaned=DataManipulation.loans_valid(convert_loan_term)
	print("Cleaned loan data")
    

	loans_repayment_df = DataReader.read_raw_loan_repayment_data(spark, job_run_env)
	print("Read raw loan repayment data")

	loan_repayment_with_timestamp= DataManipulation.loan_repayment_with_timestamp(loans_repayment_df)
	print("Added ingestion timestamp to loan repayment data")

	loan_repayment_remove_null = DataManipulation.loan_repayment_remove_nulls(loan_repayment_with_timestamp)
	print("Removed null values from loan repayment data")

	loan_repayment_amount_received = DataManipulation.loan_repayment_amount_received(loan_repayment_remove_null)
	print("Calculated loan repayment amount received")

	loan_no_repayment = DataManipulation.loan_no_repayment(loan_repayment_amount_received)
	print("Identified loans with no repayment")

	loan_repayment_last_date = DataManipulation.loan_repayment_last_date(loan_no_repayment)
	print("Identified last payment date for loans with no repayment")

	loan_repayment_cleaned = DataManipulation.loan_repayment_next_payment_date(loan_repayment_last_date)
	print("Identified next payment date for loans with no repayment")


	loan_defaulter_df=DataReader.read_raw_loan_defaulter_data(spark, job_run_env)
	print("Read raw loan defaulter data")

	loan_def_remove_nulls=DataManipulation.loan_defaulter_remove_nulls(loan_defaulter_df)
	print("Removed null values from loan defaulter data")

	loan_defaulter_delinq=DataManipulation.loan_defaulter_delinq(loan_def_remove_nulls)
	print("Processed delinquencies in loan defaulter data")

	loan_defaulter_delinq.show(5)

	loan_def_records=DataManipulation.loan_defaulter_records(loan_def_remove_nulls)
	print("Processed records in loan defaulter data")

	loan_def_records.show(5)

	loan_def_enq_details=DataManipulation.loan_defaulter_enq_details(loan_def_remove_nulls)	
	print("Processed enquiry details in loan defaulter data")

	loan_def_enq_details.show(5)
    """
	print("end of main")


