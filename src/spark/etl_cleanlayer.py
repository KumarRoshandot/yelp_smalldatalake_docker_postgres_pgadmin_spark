import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from utils import read_data_from_postgres,write_data_to_postgres

# Define SparkApp params
appName = "yelp-mindatalake-etl-job-cleanlayer"

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

# Input File Location
source_schema = 'rawlayer'
business_tablename = "yelp_academic_dataset_business"
checkin_tablename = "yelp_academic_dataset_checkin"
review_tablename = "yelp_academic_dataset_review"
tip_tablename = "yelp_academic_dataset_tip"
user_tablename = "yelp_academic_dataset_user"

def run_etl_job():

    # Read all the source data
    business_df = read_data_from_postgres(spark,source_schema,business_tablename)
    checkin_df = read_data_from_postgres(spark,source_schema,checkin_tablename)
    review_df = read_data_from_postgres(spark,source_schema,review_tablename)
    tip_df = read_data_from_postgres(spark,source_schema,tip_tablename)
    user_df = read_data_from_postgres(spark,source_schema,user_tablename)

    # Clean the yelp_academic_dataset_business data
    logging.info('Printing Schema for Business Table')
    business_df.printSchema()
    business_df = business_df.withColumn("etl_load_date",F.current_date()).drop("raw_json_text_data")

    # Clean the yelp_academic_dataset_checkin data
    logging.info('Printing Schema for Checkin Table')
    checkin_df.printSchema()
    checkin_df = checkin_df.withColumn("etl_load_date", F.current_date()).drop("raw_json_text_data")

    # Clean the yelp_academic_dataset_review data
    logging.info('Printing Schema for Review Table')
    review_df.printSchema()
    review_df = review_df.withColumn("date",F.to_date("date"))\
                         .withColumn("week",F.weekofyear("date"))\
                         .withColumn("month", F.month("date"))\
                         .withColumn("year", F.year("date"))\
                         .withColumn("etl_load_date", F.current_date()).drop("raw_json_text_data")

    # Clean the yelp_academic_dataset_tip data
    logging.info('Printing Schema for Tip Table')
    tip_df.printSchema()
    tip_df = tip_df.withColumn("etl_load_date", F.current_date()).drop("raw_json_text_data")

    # Clean the yelp_academic_dataset_user data
    logging.info('Printing Schema for User Table')
    user_df.printSchema()
    user_df = user_df.withColumn("etl_load_date", F.current_date()).drop("raw_json_text_data")

    # Write all datasets to cleanlayer schema table
    write_data_to_postgres(business_df,'cleanlayer','yelp_academic_dataset_business')
    write_data_to_postgres(checkin_df, 'cleanlayer', 'yelp_academic_dataset_checkin')
    write_data_to_postgres(review_df, 'cleanlayer', 'yelp_academic_dataset_review')
    write_data_to_postgres(tip_df, 'cleanlayer', 'yelp_academic_dataset_tip')
    write_data_to_postgres(user_df, 'cleanlayer', 'yelp_academic_dataset_user')


# Main Program Entry Point
if __name__ == '__main__':
    run_etl_job()


