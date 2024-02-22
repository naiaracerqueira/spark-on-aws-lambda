from pyspark.sql import SparkSession
from pyspark.sql.types import *
import os

"""
 Function that gets triggered when AWS Lambda is running.
"""
def spark_script():
    input_path = os.environ['INPUT_PATH']
    output_path = os.environ['OUTPUT_PATH']

    print("---------- COMEÇOU ICEBERG ----------")

    spark = SparkSession.builder \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.driver.memory", "5g") \
        .config("spark.executor.memory", "5g") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider") \
        .config("spark.hadoop.hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \
        .config("spark.jars.packages", "org.apache.iceberg") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.AwsDataCatalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.AwsDataCatalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config("spark.sql.catalog.AwsDataCatalog.warehouse", output_path ) \
        .enableHiveSupport() \
        .getOrCreate()

    print("Started Reading the CSV file from ",input_path)
    df = spark.read.options(header=True, delimiter=';').csv(input_path)
    df.printSchema()

    print("Started Writing the dataframe file to Target iceberg table ", output_path)
    df.createOrReplaceTempView("source_table")
    custom_sql_1 = f"""
        CREATE TABLE IF NOT EXISTS spark_on_lambda.accommodations_iceberg (
            id  string,
            shape  string,
            name  string,
            host_name  string,
            neighbourhood_group  string,
            neighbourhood  string,
            room_type  string,
            price  string,
            minimum_nights  string,
            number_of_reviews  string,
            last_review  string,
            reviews_per_month  string,
            calculated_host_listings_count  string,
            availability_365  string
        ) 
        LOCATION '{output_path}'
        TBLPROPERTIES ('table_type'='ICEBERG','format'='parquet');
    """

    custom_sql_2 = """
        INSERT INTO spark_on_lambda.accommodations_iceberg
        SELECT * FROM source_table;
    """

    spark.sql(custom_sql_1)
    spark.sql(custom_sql_2)

    print('OK')


if __name__ == '__main__':
    spark_script()
