from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType
from utils import read_data_from_postgres,write_data_to_postgres

# Define SparkApp params
appName = "yelp-mindatalake-etl-job-agglayer"

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName(appName) \
    .getOrCreate()

# Input File Location
source_schema = 'cleanlayer'
business_tablename = "yelp_academic_dataset_business"
checkin_tablename = "yelp_academic_dataset_checkin"
review_tablename = "yelp_academic_dataset_review"

def run_etl_job():

    # Read all the source data
    business_df = read_data_from_postgres(spark,source_schema,business_tablename)
    checkin_df = read_data_from_postgres(spark,source_schema,checkin_tablename)
    review_df = read_data_from_postgres(spark,source_schema,review_tablename)
    review_df.cache()  # Since it's going to re-use in other subsequent jobs

    # Clean the yelp_academic_dataset_business data
    business_df = business_df.withColumn("categories",F.split("categories",','))
    business_df = business_df.withColumn("categories",F.explode("categories"))\
                             .withColumn("etl_load_date",F.current_date())

    # 1. Stars per business on a weekly basis
    review_df.createOrReplaceTempView("review")
    sql = '''
    select 
        business_id,
        week,
        avg(stars) avg_stars
    from review
    group by business_id,week
    order by week;
    '''
    business_stars_weekly_df = spark.sql(sql)

    # 2. The number of check-ins of a business compared to the overall star rating.
    checkin_df = checkin_df.withColumn("no_of_checkin",F.size(F.split("date",',')))\
                           .select("business_id","no_of_checkin")
    overall_rating_sql = '''
    select 
        business_id as business_id_overall_star,
        sum(stars*star_count)/sum(star_count) overall_rating
    from 
    (	
        select 
            business_id,
            stars,
            count(1) star_count
        from review
        group by business_id,stars
    ) 
    group by business_id
    '''
    overall_rating_df = spark.sql(overall_rating_sql)
    no_of_checkin_overall_rating_df = checkin_df.join(overall_rating_df,checkin_df['business_id'] == overall_rating_df['business_id_overall_star'])\
                                                .drop("business_id_overall_star")

    # 3. Popular business categories
    business_df = business_df.withColumn("categories",F.split("categories",','))
    business_df = business_df.withColumn("categories",F.explode("categories"))
    business_df.createOrReplaceTempView("business")
    popular_categories_sql = '''
    select 
        categories,
        count(1) as category_count
    from business 
    group by 1 
    order by 2 desc
    '''
    popular_categories_df = spark.sql(popular_categories_sql)


    # Write all datasets to cleanlayer schema table
    write_data_to_postgres(business_stars_weekly_df,'agglayer','business_stars_weekly')
    write_data_to_postgres(no_of_checkin_overall_rating_df, 'agglayer', 'no_of_checkin_overall_rating')
    write_data_to_postgres(popular_categories_df, 'agglayer', 'popular_categories')


# Main Program Entry Point
if __name__ == '__main__':
    run_etl_job()


