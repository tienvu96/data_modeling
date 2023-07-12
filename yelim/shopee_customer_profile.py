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
from pyspark.sql.functions import row_number


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args
)

spark = SparkSession.builder \
.master("local") \
.appName("Word Count") \
.config("spark.jars.packages", "com.crealytics:spark-excel_2.11:0.13.7") \
.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
.getOrCreate()


path_shopee = "s3://google-sheet/Miley/google-sheet/data_ggs.xlsx"
data_shopee = pd.read_excel(path_shopee,sheet_name ="shopee_2022")
object_cols = data_shopee.select_dtypes(include=['object','long','double','datetime64[ns]']).columns
data_shopee[object_cols] = data_shopee[object_cols].astype(str)
df_shopee = spark.createDataFrame(data_shopee)


dynamic_fr= DynamicFrame.fromDF(df_shopee, glueContext, "my_dynamic_frame")


select_field_shopee  = SelectFields.apply(
    frame = dynamic_fr,
    paths = ['Ngày đặt hàng', 'Phương thức thanh toán', 'Người Mua', 'Tên Người nhận', 'Số điện thoại', 
            'Tỉnh/Thành phố', 'TP / Quận / Huyện', 'Quận', 'Địa chỉ nhận hàng'],

    transformation_ctx = "select_field_shopee"
)


change_name = ApplyMapping.apply(
    frame=select_field_shopee,
    mappings=[
        ("Ngày đặt hàng","string","created_at","string"),
        ("Phương thức thanh toán","string","payment_method","string"),
        ("Người Mua","string","source_customer_id","string"),
        ("Tên Người nhận","string","full_name","string"),
        ("Số điện thoại","string","phone","string"),
        ("Tỉnh/Thành phố","string","city","string"),
        ("TP / Quận / Huyện","string","district","string"),
        ("Quận","string","ward","string"),
        ("Địa chỉ nhận hàng","string","street","string"),
    ],
    transformation_ctx="change_name",
)

tf2_node_276_convert_df = change_name.toDF()

df_with_multiple_column = tf2_node_276_convert_df.withColumn('source_name', lit("shopee")) \
                     .withColumn('op', lit(None)) \
                     .withColumn('timestamp_dms', lit(None)) \
                     .withColumn('email', lit(None)) \
                     .withColumn('group_id', lit(None)) \
                     .withColumn('updated_at', lit(None)) \
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
                     .withColumn('marital_status', lit(None)) 
                     
uuidUdf_shopee = udf(lambda : str(uuid.uuid4()),StringType())
customer_profile_id= df_with_multiple_column.withColumn("customer_profile_id", uuidUdf_shopee())

# Remove "84" from the phone column
remove_84 = customer_profile_id.withColumn("phone", regexp_replace("phone", "^84", "")) # need to change

# Create a window specification to order by datetime in descending order
windowSpec = Window.partitionBy(remove_84["phone"]).orderBy(remove_84["created_at"].desc())

# Add a row number column based on the window specification
df_add_row_number = remove_84.withColumn("row_number", row_number().over(windowSpec))

# Filter the DataFrame to keep only the rows with row_number = 1 (newest datetime)
handle_duplicate_phone = df_add_row_number.filter(df_add_row_number["row_number"] == 1)

# Drop the row_number column
df_drop = handle_duplicate_phone.drop("row_number")

datekey =  df_drop.withColumn('datekey', date_format(col('created_at'), 'yyyyMMdd'))
customer_unified_key = datekey.withColumn("customer_unified_key",col("phone"))

# write_s3 for checking
# dynamic = DynamicFrame.fromDF(datekey, glueContext, "my_dynamic_frame")
# AmazonS3_node1688609405610 = glueContext.write_dynamic_frame.from_options(
#     frame=dynamic,
#     connection_type="s3",
#     format="csv",
#     connection_options={"path": "s3://tientest/data_output/csv/shopee2/new1/", "partitionKeys": []},
#     transformation_ctx="AmazonS3_node1688609405610",
# )

destination_delta_table_2079 = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/miley/customer_profile/") 
tf6_node_480_upsert_data = destination_delta_table_2079.alias("full_df").merge(
    customer_unified_key.alias("append_df"),
    "append_df.customer_profile_id = full_df.customer_profile_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 

tf6_node_480_upsert_data = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/miley/customer_profile/")
tf6_node_480_upsert_data.generate("symlink_format_manifest")

# Test
# dynamic_fr1.show()
# dynamic_fr1.printSchema()

job.commit()