import boto3
import pandas as pd
import datetime
import time
import json


def invoke_lambda(payload):
    # create client lambda
    lambda_client = boto3.client('lambda')

    # call another lambda
    response_lambda = lambda_client.invoke(
        FunctionName='tma-cdp-login-database-read-delta-table',  # Thay thế bằng tên của Lambda khác
        InvocationType='Event',  # Sử dụng 'Event' nếu không cần kết quả trả về
        Payload=json.dumps(payload)  # Chuyển payload thành chuỗi JSON
    )
    # check status for response lambda
    status_code = response_lambda['StatusCode']
    if status_code == 202:
        return 'Lambda invoked successfully!'
    else:
        return f'Error invoking Lambda: {status_code}'
        

def lambda_handler(event, context):
    # declare service client
    s3 = boto3.client('s3')
    glue = boto3.client('glue')
    
    customer_name = event['customer_name']


    ###############################################################
    bucket_name = "cdp-trigger-data-model"
    
    parent_folder_name = 'delta/'
    new_folder_name = customer_name + '/'
    folder_key = parent_folder_name + new_folder_name
    
    # Create the folder in the bucket and parent folder
    s3.put_object(Bucket=bucket_name, Key=folder_key)
    
    
    # check database and create the database data catalog on glue
    database_name = event['customer_name']
    try:
        # Use the Glue client to get the database
        response = glue.get_database(
            Name=database_name
        )
        
        status = {
            "sms" : f"The database {database_name} exists."
        }
        
    except glue.exceptions.EntityNotFoundException:
        response_create_db = glue.create_database(DatabaseInput={'Name': database_name})
        status = {
            "sms" : f"The database {database_name} does not exist.",
            "status_create_db": f'create {database_name} successfully'
        }
    
#     ##########___ customer_profile delta table ____ #########
#     # csv config
#     # schemas_1 = ["a1", "a2", "a3"]
#     # schemas_2 = ["b1", "b2", "b3"]
#     # schemas_3 = ["c1", "c2", "c3"]
#     # schemas_4 = ["d1", "d2", "d2"]
#     # schemas_5 = ["e1", "e2", "e3"]
    
    customer_address = ['customer_address_id', 'source_address_id', 'op', 'timestamp_dms', 'source_customer_id', 'created_at', 'updated_at', 'is_active', 'company', 'city', 'district', 'ward', 'street', 'country', 'postcode', 'prefix', 'region', 'region_id', 'phone', 'email', 'year', 'month', 'day', 'source_name']
    customer_profile = ['customer_profile_id', 'customer_unified_key', 'source_customer_id', 'op', 'timestamp_dms', 'email', 'phone', 'group_id', 'created_at', 'updated_at', 'is_active', 'disable_auto_group_change', 'created_in', 'full_name', 'firstname', 'middlename', 'lastname', 'city', 'district', 'ward', 'street', 'payment_method', 'dob', 'confirmation', 'gender', 'channels', 'source_name', 'year', 'month', 'day']
    product_interaction = ['product_interaction_id', 'source_product_id', 'product_name', 'op', 'timestamp_dms', 'entity_id', 'attribute_set_id', 'type_id', 'sku', 'created_at', 'updated_at', 'campaign_sale', 'attribute_id', 'special_from_date', 'price', 'weight', 'url', 'cost', 'size', 'color', 'description', 'season', 'year', 'month', 'day', 'source_name']
    purchase_order = ['purchase_order_id', 'source_order_id', 'state', 'status', 'refund_status', 'shipping_description', 'shipping_code', 'shipping_provider', 'shipping_method', 'cancellation_reason', 'feedback', 'delivered_date', 'total_price', 'total_payment', 'payment_method', 'shipping_fee', 'total_discount', 'street', 'city', 'district', 'ward', 'source_customer_id', 'email', 'phone', 'customer_name', 'remote_ip', 'customer_note', 'created_at', 'updated_at', 'note', 'year', 'month', 'day', 'source_name']
    purchase_order_detail = ['purchase_order_detail_id', 'source_order_detail_id', 'source_order_id', 'op', 'timestamp_dms', 'created_at', 'updated_at', 'delivered_date', 'shipping_method', 'shipping_fee', 'shipping_code', 'shipping_provider', 'shipment_ranking_code', 'email', 'phone', 'status', 'guarantee', 'sku', 'paid_price', 'discount_amount', 'item_name', 'item_variation', 'total_weight', 'product_quantily', 'other_fees', 'total_payment', 'return_initator', 'cancellation_reason', 'buyer_feedback', 'Note', 'year', 'month', 'day', 'source_name']
    
    dataframe_1 = pd.DataFrame(columns = customer_address)
    dataframe_2 = pd.DataFrame(columns = customer_profile)
    dataframe_3 = pd.DataFrame(columns = product_interaction)
    dataframe_4 = pd.DataFrame(columns = purchase_order)
    dataframe_5 = pd.DataFrame(columns = purchase_order_detail)

    tmp_bucket = "tientest"
    tmp_folder = "tmp/"
    csv_name = event["customer_name"].replace(" ", "_").lower() + "_"
    csv_date = time.strftime("%Y%m%d%H%M%S")
    
    csv_file_name_1 = f"{tmp_folder}{csv_name}{csv_date}_1.csv"
    csv_file_name_2 = f"{tmp_folder}{csv_name}{csv_date}_2.csv"
    csv_file_name_3 = f"{tmp_folder}{csv_name}{csv_date}_3.csv"
    csv_file_name_4 = f"{tmp_folder}{csv_name}{csv_date}_4.csv"
    csv_file_name_5 = f"{tmp_folder}{csv_name}{csv_date}_5.csv"
    
    csv_encode_1 = dataframe_1.to_csv(index=False).encode()
    csv_encode_2 = dataframe_2.to_csv(index=False).encode()
    csv_encode_3 = dataframe_3.to_csv(index=False).encode()
    csv_encode_4 = dataframe_4.to_csv(index=False).encode()
    csv_encode_5 = dataframe_5.to_csv(index=False).encode()
    
    # Authentication s3: need 
    s3 = boto3.client('s3', region_name='us-east-1',
                          aws_access_key_id='',
                          aws_secret_access_key='')
                          
    # Write csv to s3 bucket
    s3.put_object(Body=csv_encode_1, Bucket=tmp_bucket, Key=csv_file_name_1)
    s3.put_object(Body=csv_encode_2, Bucket=tmp_bucket, Key=csv_file_name_2)
    s3.put_object(Body=csv_encode_3, Bucket=tmp_bucket, Key=csv_file_name_3)
    s3.put_object(Body=csv_encode_4, Bucket=tmp_bucket, Key=csv_file_name_4)
    s3.put_object(Body=csv_encode_5, Bucket=tmp_bucket, Key=csv_file_name_5)
    
    
    csv_file_path_1 = f"s3://{tmp_bucket}/{csv_file_name_1}"
    csv_file_path_2 = f"s3://{tmp_bucket}/{csv_file_name_2}"
    csv_file_path_3 = f"s3://{tmp_bucket}/{csv_file_name_3}"
    csv_file_path_4 = f"s3://{tmp_bucket}/{csv_file_name_4}"
    csv_file_path_5 = f"s3://{tmp_bucket}/{csv_file_name_5}"
    
    # Delta table config
    delta_table_name_1 = "customer_profile"
    delta_table_name_2 = "purchase_order"
    delta_table_name_3 = "purchase_order_detail"
    delta_table_name_4 = "product_entity"
    delta_table_name_5 = "product_interaction"
    
    delta_table_path_1  = f"s3://{bucket_name}/{folder_key}{delta_table_name_1}/"
    delta_table_path_2  = f"s3://{bucket_name}/{folder_key}{delta_table_name_2}/"
    delta_table_path_3  = f"s3://{bucket_name}/{folder_key}{delta_table_name_3}/"
    delta_table_path_4  = f"s3://{bucket_name}/{folder_key}{delta_table_name_4}/"
    delta_table_path_5  = f"s3://{bucket_name}/{folder_key}{delta_table_name_5}/"
    
    
    # content in python
    # table 1
    scripts_1 = f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("SparkSQL")
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

df_read_csv = spark.read.format("csv").option("header", "true").load("{csv_file_path_1}")
df_read_csv.write.format("delta").mode("append").save("{delta_table_path_1}")
"""
    
    encode_python_content_1 = scripts_1.encode("utf-8")
    python_file_name_1 = f"{tmp_folder}{csv_name}{csv_date}_1.py"
    s3.put_object(Body=encode_python_content_1, Bucket=tmp_bucket, Key=python_file_name_1)
    python_file_path_1 = f"s3://{tmp_bucket}/{python_file_name_1}"
    
    # table 2
    scripts_2 = f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("SparkSQL")
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

df_read_csv = spark.read.format("csv").option("header", "true").load("{csv_file_path_2}")
df_read_csv.write.format("delta").mode("append").save("{delta_table_path_2}")
"""
    
    encode_python_content_2 = scripts_2.encode("utf-8")
    python_file_name_2 = f"{tmp_folder}{csv_name}{csv_date}_2.py"
    s3.put_object(Body=encode_python_content_2, Bucket=tmp_bucket, Key=python_file_name_2)
    python_file_path_2 = f"s3://{tmp_bucket}/{python_file_name_2}"
    
    # table 3
    scripts_3 = f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("SparkSQL")
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

df_read_csv = spark.read.format("csv").option("header", "true").load("{csv_file_path_3}")
df_read_csv.write.format("delta").mode("append").save("{delta_table_path_3}")
"""
    
    encode_python_content_3 = scripts_3.encode("utf-8")
    python_file_name_3 = f"{tmp_folder}{csv_name}{csv_date}_3.py"
    s3.put_object(Body=encode_python_content_3, Bucket=tmp_bucket, Key=python_file_name_3)
    python_file_path_3 = f"s3://{tmp_bucket}/{python_file_name_3}"
    
    # table 4
    scripts_4 = f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("SparkSQL")
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

df_read_csv = spark.read.format("csv").option("header", "true").load("{csv_file_path_4}")
df_read_csv.write.format("delta").mode("append").save("{delta_table_path_4}")
"""
    
    encode_python_content_4 = scripts_4.encode("utf-8")
    python_file_name_4 = f"{tmp_folder}{csv_name}{csv_date}_4.py"
    s3.put_object(Body=encode_python_content_4, Bucket=tmp_bucket, Key=python_file_name_4)
    python_file_path_4 = f"s3://{tmp_bucket}/{python_file_name_4}"
    
    
    # table 5
    scripts_5 = f"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = (
    SparkSession.builder
    .appName("SparkSQL")
    .config("hive.metastore.client.factory.class", "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .enableHiveSupport()
    .getOrCreate()
)

df_read_csv = spark.read.format("csv").option("header", "true").load("{csv_file_path_5}")
df_read_csv.write.format("delta").mode("append").save("{delta_table_path_5}")
"""
    
    encode_python_content_5 = scripts_5.encode("utf-8")
    python_file_name_5 = f"{tmp_folder}{csv_name}{csv_date}_5.py"
    s3.put_object(Body=encode_python_content_5, Bucket=tmp_bucket, Key=python_file_name_5)
    python_file_path_5 = f"s3://{tmp_bucket}/{python_file_name_5}"


    
    ##########___ CREATE DELTA TABLE ______ ###########
    # Check job
    response_glue_name = glue.get_jobs()
    all_job_names = [job['Name'] for job in response_glue_name['Jobs']]
    
    job_name_1 = "job_" + customer_name + "_" + delta_table_name_1
    job_name_2 = "job_" + customer_name + "_" + delta_table_name_2
    job_name_3 = "job_" + customer_name + "_" + delta_table_name_3
    job_name_4 = "job_" + customer_name + "_" + delta_table_name_4
    job_name_5 = "job_" + customer_name + "_" + delta_table_name_5
    
    job_names = [job_name_1, job_name_2, job_name_3, job_name_4, job_name_5]
    python_file_paths = [python_file_path_1, python_file_path_2, python_file_path_3, python_file_path_4, python_file_path_5]
    
    for job_name, python_file_path in zip(job_names, python_file_paths):
        if job_name not in all_job_names:
            response_create_job = glue.create_job(
                Name=job_name,
                Description='',
                Role='arn:aws:iam::321179548224:role/glue_access_redshift_s3',
                ExecutionProperty={
                    'MaxConcurrentRuns': 2
                },
                Command={
                    'Name': 'glueetl',
                    'ScriptLocation': python_file_path,
                    'PythonVersion': '3'
                },
                DefaultArguments={
                    '--additional-python-modules': 's3://tientest/library/openpyxl-3.1.2-py2.py3-none-any.whl',
                    '--datalake-formats': 'delta',
                    '--extra-jars': 's3://tientest/library/delta-core_2.12-2.1.0.jar'
                },
                MaxRetries=0,
                GlueVersion='4.0',
                NumberOfWorkers=2,
                Timeout=10,
                WorkerType='G.1X'
            )
    
            job_run_id = glue.start_job_run(JobName=job_name)['JobRunId']
        else:
            print("job đã tồn tại")


    # get information to create crawler from other lambda function
    time.sleep(240)
    folder_name_crawler = {"folder_name":customer_name}
    invoke_lambda(folder_name_crawler)
    ##############################################################
    
    status_glue_jobs = {
        f"status_job{i+1}": f"{job_name} created successfully"
        for i, job_name in enumerate([job_name_1, job_name_2, job_name_3, job_name_4, job_name_5])
        }
    
    return {
        'statusCode': 200,
        'folder_status': 'Folder created successfully.',
        'database_status': f'created {status} successfully',
        'status_glue_jobs': status_glue_jobs
        }
""" 
this lambda will create:
- database in Glue Data Catalog
- push csv and python file to tientest/tmp3 bucket in s3
- create glue job and run after 2 munites: 5  delta table will be created in cdp-tma-trigger-datamodel/delta/{customer_name}
"""
