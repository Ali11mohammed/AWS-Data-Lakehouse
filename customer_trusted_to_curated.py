import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1745613630508 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1745613630508")

# Script generated for node customer_trusted
customer_trusted_node1745613631148 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1745613631148")

# Script generated for node Join
Join_node1745613670776 = Join.apply(frame1=customer_trusted_node1745613631148, frame2=accelerometer_trusted_node1745613630508, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1745613670776")

# Script generated for node SQL Query
SqlQuery1201 = '''
SELECT DISTINCT
    customername,
    email,
    phone,
    birthday,
    serialnumber,
    registrationdate,
    lastupdatedate,
    sharewithresearchasofdate,
    sharewithpublicasofdate,
    sharewithfriendsasofdate
FROM myDataSource
'''
SQLQuery_node1745615169979 = sparkSqlQuery(glueContext, query = SqlQuery1201, mapping = {"myDataSource":Join_node1745613670776}, transformation_ctx = "SQLQuery_node1745615169979")

# Script generated for node customers_curated
customers_curated_node1745613759158 = glueContext.getSink(path="s3://ali-1897-spark-data/customers_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customers_curated_node1745613759158")
customers_curated_node1745613759158.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customers_curated")
customers_curated_node1745613759158.setFormat("json")
customers_curated_node1745613759158.writeFrame(SQLQuery_node1745615169979)
job.commit()