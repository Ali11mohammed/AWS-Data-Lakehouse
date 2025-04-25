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
accelerometer_trusted_node1745616385557 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1745616385557")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1745616541762 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1745616541762")

# Script generated for node SQL Query
SqlQuery1052 = '''
SELECT 
    t.sensorreadingtime,
    t.serialnumber,
    t.distancefromobject,
    a.x,
    a.y,
    a.z
FROM t
JOIN a
ON t.sensorreadingtime = a.timestamp
'''
SQLQuery_node1745616731710 = sparkSqlQuery(glueContext, query = SqlQuery1052, mapping = {"a":accelerometer_trusted_node1745616385557, "t":step_trainer_trusted_node1745616541762}, transformation_ctx = "SQLQuery_node1745616731710")

# Script generated for node machine_learning_curated
machine_learning_curated_node1745616832753 = glueContext.getSink(path="s3://ali-1897-spark-data/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1745616832753")
machine_learning_curated_node1745616832753.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
machine_learning_curated_node1745616832753.setFormat("json")
machine_learning_curated_node1745616832753.writeFrame(SQLQuery_node1745616731710)
job.commit()