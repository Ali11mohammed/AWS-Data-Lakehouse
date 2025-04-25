import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node customer_trusted
customer_trusted_node1745577331643 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="customer_trusted_node1745577331643")

# Script generated for node accelerometer_landing
accelerometer_landing_node1745577452222 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1745577452222")

# Script generated for node Join
Join_node1745577499350 = Join.apply(frame1=accelerometer_landing_node1745577452222, frame2=customer_trusted_node1745577331643, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1745577499350")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1745577546320 = glueContext.getSink(path="s3://ali-1897-spark-data/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_trusted_node1745577546320")
accelerometer_trusted_node1745577546320.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
accelerometer_trusted_node1745577546320.setFormat("json")
accelerometer_trusted_node1745577546320.writeFrame(Join_node1745577499350)
job.commit()