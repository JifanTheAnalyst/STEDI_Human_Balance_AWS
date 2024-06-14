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

# Script generated for node customer_trusted
customer_trusted_node1718357408384 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jiffbucket/customer/trusted/run-1718280907795-part-r-00000"], "recurse": True}, transformation_ctx="customer_trusted_node1718357408384")

# Script generated for node accelerometer_landing
accelerometer_landing_node1718356774584 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jiffbucket/accelerometer/landing/"], "recurse": True}, transformation_ctx="accelerometer_landing_node1718356774584")

# Script generated for node SQL Query
SqlQuery0 = '''
select * 
from accelerometer_landing
where user in (
select email from customer_trusted
)

'''
SQLQuery_node1718357472107 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"accelerometer_landing":accelerometer_landing_node1718356774584, "customer_trusted":customer_trusted_node1718357408384}, transformation_ctx = "SQLQuery_node1718357472107")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1718358076096 = glueContext.write_dynamic_frame.from_options(frame=SQLQuery_node1718357472107, connection_type="s3", format="json", connection_options={"path": "s3://jiffbucket/accelerometer/trusted/", "partitionKeys": []}, transformation_ctx="accelerometer_trusted_node1718358076096")

job.commit()
