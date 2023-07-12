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

# step1: read excel file
data_shopee = pd.read_excel(path_shopee,sheet_name ="shopee_2022")


# step2: redefine type of columns
# convert object into string 
object_cols = data_shopee.select_dtypes(include=['object','long','double','datetime64[ns]']).columns
data_shopee[object_cols] = data_shopee[object_cols].astype(str)

# step3: convert pandas dataframe into spark dataframe
# Create the Spark DataFrame
df_shopee = spark.createDataFrame(data_shopee)

# step4: convert spark data frame into spark dynamic data frame
dynamic_fr= DynamicFrame.fromDF(df_shopee, glueContext, "my_dynamic_frame")

# step5: select fields
select_field_shopee  = SelectFields.apply(
    frame = dynamic_fr,
    paths = ['Mã đơn hàng', 'Ngày đặt hàng', 'Trạng Thái Đơn Hàng', 'Lý do hủy', 'Nhận xét từ Người mua', 'Mã vận đơn', 
            'Đơn Vị Vận Chuyển', 'Phương thức giao hàng', 'Ngày giao hàng dự kiến', 'Tên sản phẩm', 'Giá gốc', 
            'Tổng số tiền được người bán trợ giá', 'SKU sản phẩm', 'Phương thức thanh toán'],

    transformation_ctx = "select_field_lazada"
)

# step6: rename multple columns
# Scripts generated for node ApplyMapping
change_name = ApplyMapping.apply(
    frame=select_field_shopee,
    mappings=[
        ("Mã đơn hàng","string","source_order_id","string"),
        ("Ngày đặt hàng","timestamp","created_at","string"),
        ("Trạng Thái Đơn Hàng","string","status","string"),
        ("Lý do hủy","string","cancellation_reason","string"),
        ("Nhận xét từ Người mua","string","feedback","string"),
        ("Mã vận đơn","string","shipping_code","string"),
        ("Đơn Vị Vận Chuyển","string","shipping_provider","string"),
        ("Phương thức giao hàng","string","shipping_method","string"),
        ("Ngày giao hàng dự kiến","long","estimated_delivery_date","string"),
        ("Tên sản phẩm","string","product_name","string"),
        ("Giá gốc","long","price","string"),
        ("Tổng số tiền được người bán trợ giá","string","discount_total","string"),
        ("SKU sản phẩm","string","sku","string"),
        ("Phương thức thanh toán","string","pay_method","string"),
        ],
    transformation_ctx="change_name",
)

tf2_node_276_convert_df = change_name.toDF()

tf3_node_323_add_id = tf2_node_276_convert_df.withColumn("purchase_order_detail_id", monotonically_increasing_id())

tf4_node_360_add_source_name = tf3_node_323_add_id.withColumn("source_name", lit("shopee"))

tf4_node_375_create_year_month_day = tf4_node_360_add_source_name.withColumn("year", year(tf4_node_360_add_source_name.created_at)).withColumn("month", month(tf4_node_360_add_source_name.created_at)).withColumn("day", dayofmonth(tf4_node_360_add_source_name.created_at))


destination_delta_table_2079 = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/admin/test_mmiley_purchase_order_detail/") 
tf6_node_480_upsert_data = destination_delta_table_2079.alias("full_df").merge(
    tf4_node_375_create_year_month_day.alias("append_df"),
    "append_df.purchase_order_detail_id = full_df.purchase_order_detail_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute() 

tf6_node_480_upsert_data = DeltaTable.forPath(spark, "s3a://cdp-trigger-data-model/delta/admin/test_mmiley_purchase_order_detail/")
tf6_node_480_upsert_data.generate("symlink_format_manifest")

# Test
print(df_shopee.dtypes)
# dynamic_fr1= DynamicFrame.fromDF(tf4_node_375_create_year_month_day, glueContext, "my_dynamic_frame1")
# dynamic_fr1.show()
# dynamic_fr1.printSchema()

job.commit()