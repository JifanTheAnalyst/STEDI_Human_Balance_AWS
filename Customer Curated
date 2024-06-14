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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1718359913333 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jiffbucket/accelerometer/trusted/"], "recurse": True}, transformation_ctx="accelerometer_trusted_node1718359913333")

# Script generated for node customer_trusted
customer_trusted_node1718359910149 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jiffbucket/customer/trusted/"], "recurse": True}, transformation_ctx="customer_trusted_node1718359910149")

# Script generated for node Join
Join_node1718360184152 = Join.apply(frame1=accelerometer_trusted_node1718359913333, frame2=customer_trusted_node1718359910149, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1718360184152")

# Script generated for node customer_curated
customer_curated_node1718360250335 = glueContext.write_dynamic_frame.from_options(frame=Join_node1718360184152, connection_type="s3", format="json", connection_options={"path": "s3://jiffbucket/customer/curated/", "partitionKeys": []}, transformation_ctx="customer_curated_node1718360250335")

job.commit()
