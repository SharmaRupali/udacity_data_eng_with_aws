import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customers Curated
CustomersCurated_node1696461989207 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity.rugo.spark/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomersCurated_node1696461989207",
)

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://udacity.rugo.spark/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLanding_node1",
)

# Script generated for node Customer Join
CustomerJoin_node1696460135647 = Join.apply(
    frame1=StepTrainerLanding_node1,
    frame2=CustomersCurated_node1696461989207,
    keys1=["serialNumber"],
    keys2=["serialNumber"],
    transformation_ctx="CustomerJoin_node1696460135647",
)

# Script generated for node Drop Fields
DropFields_node1696461192329 = DropFields.apply(
    frame=CustomerJoin_node1696460135647,
    paths=[
        "email",
        "phone",
        "birthDay",
        "timeStamp",
        "shareWithPublicAsOfDate",
        "shareWithResearchAsOfDate",
        "registrationDate",
        "customerName",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
        "`.serialNumber`",
    ],
    transformation_ctx="DropFields_node1696461192329",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1696460376038 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696461192329,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity.rugo.spark/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="StepTrainerTrusted_node1696460376038",
)

job.commit()
