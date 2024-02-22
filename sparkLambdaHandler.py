import boto3
import sys
import os
import subprocess
import logging


# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

def s3_script_download(s3_bucket_script: str,spark_script: str)-> None:
    s3_client = boto3.resource("s3")
    try:
        logger.info(f'Now downloading script {spark_script} in {s3_bucket_script} to /tmp')
        s3_client.Bucket(s3_bucket_script).download_file(spark_script, "/tmp/spark_script.py")
    except Exception as e :
        logger.error(f'Error downloading the script {spark_script} in {s3_bucket_script}: {e}')
    else:
        logger.info(f'Script {spark_script} successfully downloaded to /tmp')

def spark_submit(s3_bucket_script: str,spark_script: SystemError)-> None:
    """
    Submits a local Spark script using spark-submit.
    """
    try:
        logger.info(f'Spark-Submitting the Spark script {spark_script} from {s3_bucket_script}')
        subprocess.run(["spark-submit", "/tmp/spark_script.py"], check=True, env=os.environ)
    except Exception as e :
        logger.error(f'Error Spark-Submit with exception: {e}')
        raise e
    else:
        logger.info(f'Script {spark_script} successfully submitted')

def lambda_handler(event, context):
    """
    Lambda_handler is called when the AWS Lambda
    is triggered. The function is downloading file 
    from Amazon S3 location and spark submitting 
    the script in AWS Lambda
    """

    logger.info("******************Start AWS Lambda Handler************")
    s3_bucket_script = os.environ['SCRIPT_BUCKET']
    spark_script = os.environ['SPARK_SCRIPT']

    s3_script_download(s3_bucket_script, spark_script)
    
    # Set the environment variables for the Spark application
    spark_submit(s3_bucket_script, spark_script)
