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

# Script generated for node customer_landing
customer_landing_node1745569134471 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_landing", transformation_ctx="customer_landing_node1745569134471")

# Script generated for node SQL Query
SqlQuery767 = '''
SELECT * 
FROM myDataSource 
WHERE shareWithResearchAsOfDate IS NOT NULL
'''
SQLQuery_node1745569159489 = sparkSqlQuery(glueContext, query = SqlQuery767, mapping = {"myDataSource":customer_landing_node1745569134471}, transformation_ctx = "SQLQuery_node1745569159489")

# Script generated for node customer_trusted
customer_trusted_node1745569348787 = glueContext.getSink(path="s3://ali-1897-spark-data/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_trusted_node1745569348787")
customer_trusted_node1745569348787.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_trusted")
customer_trusted_node1745569348787.setFormat("json")
customer_trusted_node1745569348787.writeFrame(SQLQuery_node1745569159489)
job.commit()