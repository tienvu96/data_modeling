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

sdf_lazada = spark.createDataFrame(data_lazada)

dynamic_fr= DynamicFrame.fromDF(sdf_lazada, glueContext, "my_dynamic_frame")

select_field_lazada  = SelectFields.apply(
    frame = dynamic_fr,
    paths = ['sellerSku', 'createTime', 'updateTime', 'invoiceNumber', 'payMethod',
            'paidPrice', 'unitPrice', 'sellerDiscountTotal', 'itemName', 
            'cdShippingProvider','shippingProvider', 'status', 'buyerFailedDeliveryReason'],

    transformation_ctx = "select_field_lazada"
)

change_name = ApplyMapping.apply(
    frame=select_field_lazada,
    mappings=[
        ("sellerSku","string","sku","string"),
        ("createTime","timestamp","created_at","string"),
        ("updateTime","string","updated_at","string"),
        ("invoiceNumber","double","source_order_id","string"),
        ("payMethod","string","payment_method","string"),
        ("paidPrice","double","base_price","string"),
        ("unitPrice","string","price","string"),
        ("sellerDiscountTotal","double","discount_total","string"),
        ("itemName","string","product_name","string"),
        ("cdShippingProvider","double","size","string"),
        ("shippingProvider","string","shipping_provider","string"),
        ("status","string","status","string"),
        ("buyerFailedDeliveryReason","string","cancellation_reason","string"),
        ],
    transformation_ctx="change_name",
)

tf2_node_276_convert_df = change_name.toDF()

uuidUdf = udf(lambda : str(uuid.uuid4()),StringType())
df_with_uuid = tf2_node_276_convert_df.withColumn("purchase_order_detail_id",uuidUdf())
tf4_node_360_add_source_name = df_with_uuid.withColumn("source_name", lit("lazada"))

tf4_node_375_create_year_month_day = tf4_node_360_add_source_name.withColumn("year", year(tf4_node_360_add_source_name.created_at)).withColumn("month", month(tf4_node_360_add_source_name.created_at)).withColumn("day", dayofmonth(tf4_node_360_add_source_name.created_at))

df_with_null_cols = tf4_node_375_create_year_month_day.withColumn('op', lit(None)) \
                     .withColumn('timestamp_dms', lit(None)) \
                     .withColumn('feedback', lit(None)) \
                     .withColumn('shipping_code', lit(None)) \
                     .withColumn('shipping_method', lit(None)) \
                     .withColumn('estimated_delivery_time', lit(None)) \
                     .withColumn('item_id', lit(None)) \
                     .withColumn('quote_item_id', lit(None)) \
                     .withColumn('store_id', lit(None)) \
                     .withColumn('source_product_id', lit(None)) \
                     .withColumn('product_type', lit(None)) \
                     .withColumn('product_options', lit(None)) \
                     .withColumn('size', lit(None)) \
                     .withColumn('is_virtual', lit(None)) \
                     .withColumn('description', lit(None)) \
                     .withColumn('base_price', lit(None)) \
                     .withColumn('row_total', lit(None)) \
                     .withColumn('weight', lit(None)) 
destination_delta_table_2079 = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/admin/test_mmiley_purchase_order_detail/") 
tf6_node_480_upsert_data = destination_delta_table_2079.alias("full_df").merge(
    df_with_null_cols.alias("append_df"),
    "append_df.purchase_order_detail_id = full_df.purchase_order_detail_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 

tf6_node_480_upsert_data = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/admin/test_mmiley_purchase_order_detail/")
tf6_node_480_upsert_data.generate("symlink_format_manifest")

# Test
# print(sdf_lazada.dtypes)
# dynamic_fr1= DynamicFrame.fromDF(tf4_node_375_create_year_month_day, glueContext, "my_dynamic_frame1")
# df_with_uuid.show()
# df_with_uuid.printSchema()

job.commit()