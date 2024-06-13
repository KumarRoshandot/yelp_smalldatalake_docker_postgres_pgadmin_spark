
def read_data_from_postgres(spark,schema_name,table_name):
    try:
        # Make sure you have below URL in this format , docker-compose Alias name:port/dbName
        jdbc_url = "jdbc:postgresql://db:5432/yelp_datalake"
        connection_properties = {
            "user": "admin",
            "password": "admin",
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified"
        }
        return spark.read.jdbc(url=jdbc_url,table=f"{schema_name}.{table_name}", properties=connection_properties)
    except Exception as e:
        raise Exception(f'Exception while Reading Data from Postgres for table {table_name} - {str(e)}')

def write_data_to_postgres(df,schema_name,table_name,mode='overwrite'):
    try:
        # Make sure you have below URL in this format , docker-compose Alias name:port/dbName
        jdbc_url = "jdbc:postgresql://db:5432/yelp_datalake"
        connection_properties = {
            "user": "admin",
            "password": "admin",
            "driver": "org.postgresql.Driver",
            "stringtype": "unspecified"
        }
        df.write.option("truncate","true").jdbc(url=jdbc_url, table=f"{schema_name}.{table_name}", mode=mode, properties=connection_properties)

    except Exception as e:
        raise Exception(f'Exception while Writing Data into Postgres for table {table_name}- {str(e)}')
