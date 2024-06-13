import os
import sys
import datetime
import argparse
import logging

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from utils import write_data_to_postgres
from pyspark.sql.types import DateType

parser = argparse.ArgumentParser()
parser.add_argument('--source_file_path', type=str, required=True)
args = parser.parse_args()

# Define SparkApp params
appName = "yelp-mindatalake-etl-job-rawlayer"

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

# Input File Location
source_path = args.source_file_path
business_file_path = os.path.join(source_path, "yelp_academic_dataset_business.json")
checkin_file_path = os.path.join(source_path, "yelp_academic_dataset_checkin.json")
review_file_path = os.path.join(source_path, "yelp_academic_dataset_review.json")
tip_file_path = os.path.join(source_path, "yelp_academic_dataset_tip.json")
user_file_path = os.path.join(source_path, "yelp_academic_dataset_user.json")


def read_source_data(json_path):
    return spark.read.json(json_path)

def run_etl_job():

    # Read all the source data
    business_df = read_source_data(business_file_path)
    checkin_df = read_source_data(checkin_file_path)
    review_df = read_source_data(review_file_path)
    tip_df = read_source_data(tip_file_path)
    user_df = read_source_data(user_file_path)

    # Load the yelp_academic_dataset_business data
    logging.info('Printing Schema for Business File')
    business_df.printSchema()
    business_df = business_df.withColumn("attributes",F.to_json("attributes"))\
                             .withColumn("hours",F.to_json("hours"))\
                             .withColumn("etl_load_date",F.current_date())
    business_df = business_df.withColumn("raw_json_text_data", F.to_json(F.struct([business_df[x] for x in business_df.columns])))

    # Load the yelp_academic_dataset_checkin data
    logging.info('Printing Schema for Checkin File')
    checkin_df.printSchema()
    checkin_df = checkin_df.withColumn("etl_load_date", F.current_date())
    checkin_df = checkin_df.withColumn("raw_json_text_data",F.to_json(F.struct([checkin_df[x] for x in checkin_df.columns])))

    # Load the yelp_academic_dataset_review data
    logging.info('Printing Schema for Review File')
    review_df.printSchema()
    review_df = review_df.withColumn("etl_load_date", F.current_date())
    review_df = review_df.withColumn("raw_json_text_data",F.to_json(F.struct([review_df[x] for x in review_df.columns])))


    # Load the yelp_academic_dataset_tip data
    logging.info('Printing Schema for Tip File')
    tip_df.printSchema()
    tip_df = tip_df.withColumn("etl_load_date", F.current_date())
    tip_df = tip_df.withColumn("raw_json_text_data",F.to_json(F.struct([tip_df[x] for x in tip_df.columns])))

    # Load the yelp_academic_dataset_user data
    logging.info('Printing Schema for User File')
    user_df.printSchema()
    user_df = user_df.withColumn("etl_load_date", F.current_date())
    user_df = user_df.withColumn("raw_json_text_data",F.to_json(F.struct([user_df[x] for x in user_df.columns])))

    # Write all datasets to rawlayer schema table
    write_data_to_postgres(business_df,'rawlayer','yelp_academic_dataset_business')
    write_data_to_postgres(checkin_df, 'rawlayer', 'yelp_academic_dataset_checkin')
    write_data_to_postgres(review_df, 'rawlayer', 'yelp_academic_dataset_review')
    write_data_to_postgres(tip_df, 'rawlayer', 'yelp_academic_dataset_tip')
    write_data_to_postgres(user_df, 'rawlayer', 'yelp_academic_dataset_user')


# Main Program Entry Point
if __name__ == '__main__':
    run_etl_job()


