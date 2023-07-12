import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from delta import *
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import lit
from pyspark.sql import Window
from pyspark.sql.functions import dense_rank
import pandas as pd
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, StructField
from pyspark.sql.functions import col
import uuid
from pyspark.sql.functions import udf, row_number
from pyspark.sql.functions import sum,avg,max
from pyspark.sql.functions import regexp_replace, to_timestamp, date_format




args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark = SparkSession.builder \
.master("local") \
.appName("Word Count") \
.config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.13.7") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.getOrCreate()

path_lazada = "s3://google-sheet/Miley/google-sheet/data_ggs.xlsx"
data_lazada = pd.read_excel(path_lazada,sheet_name ="lazada_2020_21_22")
object_cols = data_lazada.select_dtypes(include=['object','long','double','datetime64[ns]']).columns
data_lazada[object_cols] = data_lazada[object_cols].astype(str)

# Create the Spark DataFrame
sdf_lazada = spark.createDataFrame(data_lazada)
dynamic_fr= DynamicFrame.fromDF(sdf_lazada, glueContext, "my_dynamic_frame")

select_field_lazada  = SelectFields.apply(
    frame = dynamic_fr,
    paths = ['createTime', 'updateTime','customerName', 
            'customerEmail','shippingName','shippingAddress',
            'shippingAddress3','shippingAddress4',
            'shippingAddress5','billingPhone','payMethod'],

    transformation_ctx = "select_field_lazada"
)

# # Scripts generated for node ApplyMapping
change_name = ApplyMapping.apply(
    frame = select_field_lazada,
    mappings=[
        ("createTime","string","created_at","string"),
        ("updateTime","string","updated_at","string"),
        ("customerName","string","source_customer_id","string"),
        ("customerEmail","string","email","string"),
        ("shippingName","string","full_name","string"),
        ("shippingAddress","string","street","string"),
        ("shippingAddress3","string","city","string"),
        ("shippingAddress4","string","district","string"),
        ("shippingAddress5","string","ward","string"),
        ("billingPhone","string","phone","string"),
        ("payMethod","string","payment_method","string"),
        ],
    transformation_ctx="change_name",
)
change_name_to_df = change_name.toDF()

# Scripts generated for node Identifier
df_with_multiple_column = change_name_to_df.withColumn('source_name', lit("lazada")) \
                     .withColumn('customer_unified_key', lit(None)) \
                     .withColumn('op', lit(None)) \
                     .withColumn('timestamp_dms', lit(None)) \
                     .withColumn('group_id', lit(None)) \
                     .withColumn('is_active', lit(None)) \
                     .withColumn('disable_auto_group_change', lit(None)) \
                     .withColumn('created_in', lit(None)) \
                     .withColumn('firstname', lit(None)) \
                     .withColumn('middlename', lit(None)) \
                     .withColumn('lastname', lit(None)) \
                     .withColumn('dob', lit(None)) \
                     .withColumn('confirmation', lit(None)) \
                     .withColumn('gender', lit(None)) \
                     .withColumn('year', lit(None)) \
                     .withColumn('month', lit(None)) \
                     .withColumn('day', lit(None)) \
                     .withColumn('marital_status', lit(None)) \
                     .withColumn('channels', lit("online")) \
                     .withColumn('customer_rank', lit(None)) 

uuidUdf_shopee = udf(lambda : str(uuid.uuid4()),StringType())
customer_profile_id= df_with_multiple_column.withColumn("customer_profile_id", uuidUdf_shopee())

# Remove "84" from the phone column
remove_84 = customer_profile_id.withColumn("phone", regexp_replace("phone", "^84", ""))

# Create a window specification to order by datetime in descending order
windowSpec = Window.partitionBy(remove_84["phone"]).orderBy(remove_84["created_at"].desc())

# Add a row number column based on the window specification
df_add_row_number = remove_84.withColumn("row_number", row_number().over(windowSpec))

# Filter the DataFrame to keep only the rows with row_number = 1 (newest datetime)
handle_duplicate_phone = df_add_row_number.filter(df_add_row_number["row_number"] == 1)

# Drop the row_number column
df_drop = handle_duplicate_phone.drop("row_number")

# handle datetime
# df_convert_timestamp_for_created_at = df_drop.withColumn("created_at", to_timestamp("created_at", "dd MMM yyyy HH:mm")) \
#     .withColumn("created_at", date_format("created_at", "yyyy-mm-dd HH:mm:ss"))
# df_convert_timestamp_for_updated_at = df_convert_timestamp_for_created_at.withColumn("updated_at", to_timestamp("updated_at", "dd MMM yyyy HH:mm")) \
#     .withColumn("updated_at", date_format("updated_at", "yyyy-mm-dd HH:mm:ss"))
    
# from pyspark.sql.functions import to_timestamp, date_format, col

df_format_date = df_drop.withColumn("created_at", date_format(to_timestamp(col("created_at"), "dd MMM yyyy HH:mm"), "yyyy-MM-dd HH:mm:ss")) \
      .withColumn("updated_at", date_format(to_timestamp(col("updated_at"), "dd MMM yyyy HH:mm"), "yyyy-MM-dd HH:mm:ss"))


customer_unified_key = df_format_date.withColumn("customer_unified_key",col("phone"))
datekey =  customer_unified_key.withColumn('datekey', date_format(col('created_at'), 'yyyyMMdd'))

# # Read current as Delta Table
destination_delta_table_2079 = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/miley/customer_profile_v1/") 
# # Upsert process
tf6_node_480_upsert_data = destination_delta_table_2079.alias("full_df").merge(
    datekey.alias("append_df"),
    "append_df.customer_profile_id = full_df.customer_profile_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 

# # # Generate new Manifest file
tf6_node_480_upsert_data = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/miley/customer_profile_v1/")
tf6_node_480_upsert_data.generate("symlink_format_manifest")

# print(sdf_lazada.dtypes)
# dynamic_fr1= DynamicFrame.fromDF(tf4_node_375_create_year_month_day, glueContext, "my_dynamic_frame1")
# group_df.printSchema()

job.commit()