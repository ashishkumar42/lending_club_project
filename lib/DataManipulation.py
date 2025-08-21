from pyspark.sql.functions import current_timestamp
from pyspark.sql.functions import concat_ws, sha2, regexp_replace, length, col, when 

def generate_key(initial_raw_data_df):
    initial_raw_data_df_with_key=initial_raw_data_df.withColumn("key",sha2(concat_ws("_",*["emp_title", "emp_length","home_ownership", "annual_inc", "zip_code", "addr_state", "grade", "sub_grade","verification_status"]),256))
    return initial_raw_data_df_with_key

# function to insert a new column named as ingestion date(current time)

def add_ingestion_timestamp_customer(raw_customer_df):
    customer_df_with_timestamp=raw_customer_df.withColumn("ingestion_timestamp",current_timestamp())
    return customer_df_with_timestamp

#This function removes duplicate rows from customer dataframe

def remove_duplicates_customer(customer_df_with_timestamp):
    customer_df_unique=customer_df_with_timestamp.dropDuplicates()
    return customer_df_unique

# This function filters out rows with null values in the annual_income column.
def remove_null_annual_income(customer_df_unique):
    customer_df_without_null_annual_income=customer_df_unique.filter("annual_income is not null")
    return customer_df_without_null_annual_income   

# This function converts the emp_length column to an integer type by removing non-digit characters.
def convert_emp_length_to_int(customer_df_without_null_annual_income):
    customer_df_converted_emp_length=customer_df_without_null_annual_income.withColumn("emp_length",regexp_replace("emp_length","\\D","").cast("int"))
    return customer_df_converted_emp_length

# This function fills null values in the emp_length column with the average emp_length.
def null_with_avg_emp_length(customer_df_converted_emp_length):
    avg_emp_length=customer_df_converted_emp_length.agg({"emp_length":"avg"}).collect()[0][0]
    customer_df_filled=customer_df_converted_emp_length.fillna({"emp_length":avg_emp_length})
    return customer_df_filled   

def clean_customer_address(null_with_avg_emp_length_df):
    cleaned_address_df=null_with_avg_emp_length_df.withColumn("address_state",when(length(col("address_state"))==2,col("address_state")) \
                        .otherwise("NA"))
    return cleaned_address_df

def write_cleaned_customer_data(cleaned_address_df):
    output_path = "data/cleaned/cleaned_customer_data_csv"
    cleaned_address_df.write.mode("overwrite").csv(output_path)
    

def add_ingestion_timestamp_loan(loan_raw_df):
    loan_df_with_timestamp=loan_raw_df.withColumn("ingestion_timestamp",current_timestamp())
    return loan_df_with_timestamp

def loans_df_remove_nulls(loan_df_with_timestamp):
    columns_to_check=["loan_amount", "funded_amount", "loan_term", "interest_rate", "monthly_installment", "issue_date", "loan_status", "loan_purpose"]
    loans_df_remove_nulls=loan_df_with_timestamp.dropna(subset=columns_to_check)
    return loans_df_remove_nulls

def convert_loan_term(loan_df_remove_nulls):
    loan_df_converted_term=loan_df_remove_nulls.withColumn("loan_term_in_years",
    (regexp_replace("loan_term","\\D","")/12).cast("int"))
    return loan_df_converted_term   

def loans_valid(loan_df_converted_term):
    valid_purpose=["debt_consolidation", "credit_card", "home_improvement", "other", "major_purchase", "medical", "small_business", "car", "vacation", "moving", "house", "wedding", "renewable_energy", "educational"]
    loans_valid_df=loan_df_converted_term.withColumn("loan_purpose", when(col("loan_purpose").isin(valid_purpose), \
                        col("loan_purpose")).otherwise("other"))
    return loans_valid_df

def loan_repayment_with_timestamp(raw_loan_repayment_df):
    loan_repayment_with_timestamp_df=raw_loan_repayment_df.withColumn("ingestion_timestamp",current_timestamp())
    return loan_repayment_with_timestamp_df

def loan_repayment_remove_nulls(loan_repayment_with_timestamp_df):
    columns_to_check = ["total_principal_received", "total_interest_received", "total_late_fee_received", "total_payment_received", "last_payment_amount"]
    loan_repayment_remove_nulls_df=loan_repayment_with_timestamp_df.dropna(subset=columns_to_check)
    return loan_repayment_remove_nulls_df

def loan_repayment_amount_received(loan_repayment_remove_nulls_df):
    loan_repayment_amount_received_df=loan_repayment_remove_nulls_df.withColumn("total_payment_received", \
        when((col("total_payment_received")==0 ) & (col("total_principal_received")!=0), \
        col("total_principal_received") + col("total_interest_received") + col("total_late_fee_received")) \
        .otherwise(col("total_payment_received")))
    return loan_repayment_amount_received_df

def loan_no_repayment(loan_repayment_amount_received_df):
    loan_no_repayment_df=loan_repayment_amount_received_df.filter("total_payment_received != 0")
    return loan_no_repayment_df

def loan_repayment_last_date(loan_no_repayment_df):
    loan_repayment_last_date_df=loan_no_repayment_df.withColumn("last_payment_date", \
        when(col("last_payment_date")==0.0, None).otherwise(col("last_payment_date")))
    return loan_repayment_last_date_df

def loan_repayment_next_payment_date(loan_repayment_last_date_df):
    loan_repayment_next_payment_date_df=loan_repayment_last_date_df.withColumn("next_payment_date", \
        when(col("next_payment_date")==0.0, None).otherwise(col("next_payment_date")))
    return loan_repayment_next_payment_date_df

def loan_defaulter_remove_nulls(raw_loan_defaulter_df):
    loan_defaulter_remove_nulls_df=raw_loan_defaulter_df.withColumn("delinq_2yrs", \
        col("delinq_2yrs").cast("int")).fillna(0,subset=["delinq_2yrs"])
    return loan_defaulter_remove_nulls_df

def loan_defaulter_delinq(loan_defaulter_remove_nulls_df):
    loan_defaulter_delinq_df=loan_defaulter_remove_nulls_df.filter("delinq_2yrs >0 or cast(mths_since_last_delinq as int) >0") \
        .select("member_id","delinq_2yrs", "delinq_amnt", "mths_since_last_delinq") 
    return loan_defaulter_delinq_df

def loan_defaulter_records(loan_defaulter_remove_nulls_df):
    loan_defaulter_records_df=loan_defaulter_remove_nulls_df.filter("inq_last_6mths > 0 or pub_rec > 0 or pub_rec_bankruptcies > 0") \
        .select("member_id")
    return loan_defaulter_records_df

def loan_defaulter_enq_details(loan_defaulter_remove_nulls_df):
    loan_defaulter_enq_details_df=loan_defaulter_remove_nulls_df.withColumn("inq_last_6mths", col("inq_last_6mths").cast("int")) \
        .withColumn("pub_rec_bankruptcies", col("pub_rec_bankruptcies").cast("int")) \
        .withColumn("pub_rec", col("pub_rec").cast("int")) \
        .fillna(0, subset=["inq_last_6mths", "pub_rec_bankruptcies", "pub_rec"]) \
        .select("member_id", "inq_last_6mths", "pub_rec_bankruptcies", "pub_rec")
    return loan_defaulter_enq_details_df

