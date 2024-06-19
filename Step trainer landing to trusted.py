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

# Script generated for node customer_curated
customer_curated_node1718367786242 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jiffbucket/cus/"], "recurse": True}, transformation_ctx="customer_curated_node1718367786242")

# Script generated for node step_trainer
step_trainer_node1718367787128 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jiffbucket/step_trainer/landing/"], "recurse": True}, transformation_ctx="step_trainer_node1718367787128")

# Script generated for node SQL Query
SqlQuery0 = '''
select * from step_trainer
where serialnumber in (select serialnumber from customer_curated
)
'''
SQLQuery_node1718367793073 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"step_trainer":step_trainer_node1718367787128, "customer_curated":customer_curated_node1718367786242}, transformation_ctx = "SQLQuery_node1718367793073")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1718802449129 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1718367793073, connection_type="s3", format="json", connection_options={"path": "s3://jiffbucket/step_trainer/trusted/", "compression": "snappy", "partitionKeys": []}, transformation_ctx="step_trainer_trusted_node1718802449129")

job.commit()
