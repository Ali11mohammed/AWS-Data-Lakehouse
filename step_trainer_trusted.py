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

# Script generated for node customers_curated
customers_curated_node1745614082273 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customers_curated", transformation_ctx="customers_curated_node1745614082273")

# Script generated for node step_trainer_landing
step_trainer_landing_node1745614263189 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1745614263189")

# Script generated for node SQL Query
SqlQuery834 = '''
SELECT stl.*
FROM stl
JOIN cus
ON stl.serialnumber = cus.serialnumber
'''
SQLQuery_node1745614333754 = sparkSqlQuery(glueContext, query = SqlQuery834, mapping = {"cus":customers_curated_node1745614082273, "stl":step_trainer_landing_node1745614263189}, transformation_ctx = "SQLQuery_node1745614333754")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1745614419859 = glueContext.getSink(path="s3://ali-1897-spark-data/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1745614419859")
step_trainer_trusted_node1745614419859.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1745614419859.setFormat("json")
step_trainer_trusted_node1745614419859.writeFrame(SQLQuery_node1745614333754)
job.commit()