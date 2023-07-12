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

path_shopee = "s3://google-sheet/Miley/google-sheet/data_ggs.xlsx"
data_shopee = pd.read_excel(path_shopee,sheet_name ="shopee_2022")
object_cols = data_shopee.select_dtypes(include=['object','long','double','datetime64[ns]']).columns
data_shopee[object_cols] = data_shopee[object_cols].astype(str)

# Create the Spark DataFrame
sdf_shopee = spark.createDataFrame(data_shopee)
dynamic_fr= DynamicFrame.fromDF(sdf_shopee, glueContext, "my_dynamic_frame")

select_field_shopee  = SelectFields.apply(
    frame = dynamic_fr,
    paths = ['Mã đơn hàng', 'Ngày đặt hàng', 'Trạng Thái Đơn Hàng', 'Lý do hủy', 'Nhận xét từ Người mua', 'Mã vận đơn', 
            'Đơn Vị Vận Chuyển', 'Phương thức giao hàng', 'Thời gian giao hàng', 'Trạng thái Trả hàng/Hoàn tiền', 'Tổng giá trị đơn hàng (VND)', 
            'Tổng số tiền người mua thanh toán','Phương thức thanh toán','Người Mua','Tên Người nhận','Số điện thoại','Tỉnh/Thành phố','TP / Quận / Huyện',
            'Quận','Địa chỉ nhận hàng','Ghi chú'],

    transformation_ctx = "select_field_shopee"
)

# # Scripts generated for node ApplyMapping
change_name = ApplyMapping.apply(
    frame=select_field_shopee,
    mappings=[
        ("Mã đơn hàng","string","source_order_id","string"),
        ("Ngày đặt hàng","string","created_at","string"),
        ("Trạng Thái Đơn Hàng","string","status","string"),
        ("Lý do hủy","string","cancellation_reason","string"),
        ("Nhận xét từ Người mua","string","feedback","string"),
        ("Mã vận đơn","string","shipping_code","string"),
        ("Đơn Vị Vận Chuyển","string","shipping_provider","string"),
        ("Phương thức giao hàng","string","shipping_method","string"),
        ("Thời gian giao hàng","string","delivered_date","string"),
        ("Trạng thái Trả hàng/Hoàn tiền","string","refund_status","string"),
        ("Tổng giá trị đơn hàng (VND)","string","total_price","string"),
        ("Tổng số tiền người mua thanh toán","string","total_payment","string"),
        ("Phương thức thanh toán","string","payment_method","string"),
        ("Người Mua","string","source_customer_id","string"),
        ("Tên Người nhận","string","customer_name","string"),
        ("Số điện thoại","string","phone","string"),
        ("Tỉnh/Thành phố","string","city","string"),
        ("TP / Quận / Huyện","string","district","string"),
        ("Quận","string","ward","string"),
        ("Địa chỉ nhận hàng","string","street","string"),
        ("Ghi chú","string","note","string"),
        ],
    transformation_ctx="change_name",
)
change_name_to_df = change_name.toDF()

group_df = change_name_to_df.groupBy("source_order_id") \
    .agg(max("created_at").alias("created_at"), \
         max("status").alias("status"), \
         max("cancellation_reason").alias("cancellation_reason"), \
         max("feedback").alias("feedback"), \
         max("shipping_code").alias("shipping_code"), \
         max("shipping_provider").alias("shipping_provider"), \
         max("shipping_method").alias("shipping_method"), \
         max("delivered_date").alias("delivered_date"), \
         max("refund_status").alias("refund_status"), \
         sum("total_price").alias("total_price"), \
         sum("total_payment").alias("total_payment"), \
         max("payment_method").alias("payment_method"), \
         max("source_customer_id").alias("source_customer_id"), \
         max("customer_name").alias("customer_name"), \
         max("phone").alias("phone"), \
         max("city").alias("city"), \
         max("district").alias("district"), \
         max("ward").alias("ward"), \
         max("street").alias("street"), \
         max("note").alias("note") 
     )

df_with_multiple_column = group_df.withColumn('source_name', lit("shopee")) \
                     .withColumn('state', lit(None)) \
                     .withColumn('shipping_description', lit(None)) \
                     .withColumn('shipping_fee', lit(None)) \
                     .withColumn('total_discount', lit(None)) \
                     .withColumn('email', lit(None)) \
                     .withColumn('remote_ip', lit(None)) \
                     .withColumn('customer_note', lit(None)) \
                     .withColumn('updated_at', lit(None)) \
                     .withColumn('year', lit(None)) \
                     .withColumn('month', lit(None)) \
                     .withColumn('day', lit(None)) \
                     .withColumn('datekey',lit(None))

# generate id for purchase_order 
uuidUdf_shopee = udf(lambda : str(uuid.uuid4()),StringType())
customer_profile_id= df_with_multiple_column.withColumn("purchase_order_id", uuidUdf_shopee())

# handle duplicate phone + latest day
# Remove "84" from the phone column
remove_84 = customer_profile_id.withColumn("phone", regexp_replace("phone", "^84", "0"))
# Create a window specification to order by datetime in descending order
windowSpec = Window.partitionBy(remove_84["phone"]).orderBy(remove_84["created_at"].desc())

# Add a row number column based on the window specification
df_add_row_number = remove_84.withColumn("row_number", row_number().over(windowSpec))

# Filter the DataFrame to keep only the rows with row_number = 1 (newest datetime)
handle_duplicate_phone = df_add_row_number.filter(df_add_row_number["row_number"] == 1)

# Drop the row_number column
df_drop = handle_duplicate_phone.drop("row_number")
datekey =  df_drop.withColumn('datekey', date_format(col('created_at'), 'yyyyMMdd'))



# # Read current as Delta Table
# destination_delta_table_2079 = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/miley/tien_purchase_order_partition/") 
# # # Upsert process
# tf6_node_480_upsert_data = destination_delta_table_2079.alias("full_df").merge(
#     df_with_multiple_column.alias("append_df"),
#     "append_df.purchase_order_id = full_df.purchase_order_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 

# # # # Generate new Manifest file
# tf6_node_480_upsert_data = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/miley/tien_purchase_order_partition/")
# tf6_node_480_upsert_data.generate("symlink_format_manifest")

# print(sdf_lazada.dtypes)
# dynamic_fr1= DynamicFrame.fromDF(tf4_node_375_create_year_month_day, glueContext, "my_dynamic_frame1")
datekey.show(2)
# group_df.printSchema()

job.commit()