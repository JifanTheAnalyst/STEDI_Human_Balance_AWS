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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1718813770596 = glueContext.create_dynamic_frame.from_catalog(database="jiffdb", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1718813770596")

# Script generated for node step_trainer
step_trainer_node1718367787128 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jiffbucket/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_node1718367787128")

# Script generated for node customer_curated
customer_curated_node1718810986865 = glueContext.create_dynamic_frame.from_catalog(database="jiffdb", table_name="customer_curated", transformation_ctx="customer_curated_node1718810986865")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from step_trainer
where serialnumber in (select serialnumber from customer_curated
)
'''
SQLQuery_node1718367793073 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer":step_trainer_node1718367787128, "customer_curated":customer_curated_node1718810986865}, transformation_ctx = "SQLQuery_node1718367793073")

# Script generated for node Join
Join_node1718813782071 = Join.apply(frame1=AWSGlueDataCatalog_node1718813770596, frame2=SQLQuery_node1718367793073, keys1=["timestamp"], keys2=["sensorReadingTime"], transformation_ctx="Join_node1718813782071")


# Script generated for node machine_learning_curated
machine_learning_curated_node1718813864174 = glueContext.getSink(path="s3://jiffbucket/ma/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1718813864174")
machine_learning_curated_node1718813864174.setCatalogInfo(catalogDatabase="jiffdb",catalogTableName="machine_learning_curated_copy")
machine_learning_curated_node1718813864174.setFormat("json")
machine_learning_curated_node1718813864174.writeFrame(Join_node1718813782071)
job.commit()
