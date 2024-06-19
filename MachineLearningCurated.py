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

# Script generated for node customer_curated
customer_curated_node1718367786242 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jiffbucket/cus/"], "recurse": True}, transformation_ctx="customer_curated_node1718367786242")

# Script generated for node step_trainer
step_trainer_node1718367787128 = glueContext.create_dynamic_frame.from_options(format_options={"multiline": False}, connection_type="s3", format="json", connection_options={"paths": ["s3://jiffbucket/step_trainer/trusted/"], "recurse": True}, transformation_ctx="step_trainer_node1718367787128")

# Script generated for node Change Schema cus
ChangeSchemacus_node1718802990724 = ApplyMapping.apply(frame=customer_curated_node1718367786242, mappings=[("serialnumber", "string", "cus_serialnumber", "string"), ("z", "double", "z", "double"), ("birthday", "string", "birthday", "string"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"), ("registrationdate", "long", "registrationdate", "long"), ("customername", "string", "customername", "string"), ("user", "string", "user", "string"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"), ("y", "double", "y", "double"), ("x", "double", "x", "double"), ("timestamp", "long", "timestamp", "long"), ("email", "string", "email", "string"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("phone", "string", "phone", "string")], transformation_ctx="ChangeSchemacus_node1718802990724")

# Script generated for node Change Schema ste
ChangeSchemaste_node1718802991789 = ApplyMapping.apply(frame=step_trainer_node1718367787128, mappings=[("sensorreadingtime", "long", "sensorreadingtime", "long"), ("serialnumber", "string", "ste_serialnumber", "string"), ("distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="ChangeSchemaste_node1718802991789")

# Script generated for node Join
Join_node1718803188381 = Join.apply(frame1=ChangeSchemacus_node1718802990724, frame2=ChangeSchemaste_node1718802991789, keys1=["cus_serialnumber"], keys2=["ste_serialnumber"], transformation_ctx="Join_node1718803188381")

# Script generated for node machine_learning_curated
machine_learning_curated_node1718803245263 = glueContext.write_dynamic_frame.from_options(frame=Join_node1718803188381, connection_type="s3", format="json", connection_options={"path": "s3://jiffbucket/customer/machine_learning/", "partitionKeys": []}, transformation_ctx="machine_learning_curated_node1718803245263")

job.commit()
