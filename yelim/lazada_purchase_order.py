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
from pyspark.sql.functions import udf
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
    paths = ['deliveryType', 'createTime', 'updateTime', 'invoiceNumber','deliveredDate','customerName', 
            'customerEmail','shippingName','shippingAddress','shippingAddress3','shippingAddress4',
            'shippingAddress5','billingPhone','payMethod','paidPrice', 'unitPrice', 'sellerDiscountTotal',
            'shippingFee','status','buyerFailedDeliveryReturnInitiator','buyerFailedDeliveryReason',
            'sellerNote'],

    transformation_ctx = "select_field_lazada"
)

# # Scripts generated for node ApplyMapping
change_name = ApplyMapping.apply(
    frame = select_field_lazada,
    mappings=[
        ("deliveryType","string","shipping_method","string"),
        ("createTime","string","created_at","string"),
        ("updateTime","string","updated_at","string"),
        ("invoiceNumber","string","source_order_id","string"),
        ("deliveredDate","string","delivered_date","string"),
        ("customerName","string","source_customer_id","string"),
        ("customerEmail","string","email","string"),
        ("shippingName","string","customer_name","string"),
        ("shippingAddress","string","street","string"),
        ("shippingAddress3","string","city","string"),
        ("shippingAddress4","string","district","string"),
        ("shippingAddress5","string","ward","string"),
        ("billingPhone","string","phone","string"),
        ("payMethod","string","payment_method","string"),
        ("paidPrice","string","total_payment","string"),
        ("unitPrice","string","total_price","string"),
        ("sellerDiscountTotal","string","total_discount","string"),
        ("shippingFee","string","shipping_fee","string"),
        ("status","string","status","string"),
        ("buyerFailedDeliveryReturnInitiator","string","refund_status","string"),
        ("buyerFailedDeliveryReason","string","cancellation_reason","string"),
        ("sellerNote","string","note","string"),
        ],
    transformation_ctx="change_name",
)
change_name_to_df = change_name.toDF()
group_df = change_name_to_df.groupBy("source_order_id") \
    .agg(max("status").alias("status"), \
         max("refund_status").alias("refund_status"), \
         max("shipping_method").alias("shipping_method"), \
         max("delivered_date").alias("delivered_date"), \
         max("cancellation_reason").alias("cancellation_reason"), \
         sum("total_price").alias("total_price"), \
         sum("total_payment").alias("total_payment"), \
         max("payment_method").alias("payment_method"), \
         sum("shipping_fee").alias("shipping_fee"), \
         sum("total_discount").alias("total_discount"), \
         max("street").alias("street"), \
         max("city").alias("city"), \
         max("district").alias("district"), \
         max("ward").alias("ward"), \
         max("source_customer_id").alias("source_customer_id"), \
         max("email").alias("email"), \
         max("phone").alias("phone"), \
         max("customer_name").alias("customer_name"), \
         max("created_at").alias("created_at"), \
         max("updated_at").alias("updated_at"), \
         max("note").alias("note")  
     )

# Scripts generated for node Identifier
uuidUdf = udf(lambda : str(uuid.uuid4()),StringType())
df_with_uuid = group_df.withColumn("purchase_order_id",uuidUdf())

df_with_multiple_column = df_with_uuid.withColumn('source_name', lit("lazada")) \
                     .withColumn('state', lit(None)) \
                     .withColumn('shipping_description', lit(None)) \
                     .withColumn('shipping_code', lit(None)) \
                     .withColumn('shipping_provider', lit(None)) \
                     .withColumn('feedback', lit(None)) \
                     .withColumn('remote_ip', lit(None)) \
                     .withColumn('customer_note', lit(None)) \
                     .withColumn('year', lit(None)) \
                     .withColumn('month', lit(None)) \
                     .withColumn('day', lit(None))
                     
datekey =  df_with_multiple_column.withColumn('datekey', date_format(col('created_at'), 'yyyyMMdd'))

# # Read current as Delta Table
destination_delta_table_2079 = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/miley/miley_purchase_order/") 
# # Upsert process
tf6_node_480_upsert_data = destination_delta_table_2079.alias("full_df").merge(
    datekey.alias("append_df"),
    "append_df.purchase_order_id = full_df.purchase_order_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 

# # # Generate new Manifest file
tf6_node_480_upsert_data = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/miley/miley_purchase_order/")
tf6_node_480_upsert_data.generate("symlink_format_manifest")

# print(sdf_lazada.dtypes)
# dynamic_fr1= DynamicFrame.fromDF(tf4_node_375_create_year_month_day, glueContext, "my_dynamic_frame1")
# group_df.show()
# group_df.printSchema()

job.commit()