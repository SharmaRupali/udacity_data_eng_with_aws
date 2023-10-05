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

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1696463479070 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1696463479070",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1696463480770 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1696463480770",
)

# Script generated for node Join
Join_node1696463550611 = Join.apply(
    frame1=StepTrainerTrusted_node1696463479070,
    frame2=AccelerometerTrusted_node1696463480770,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1696463550611",
)

# Script generated for node Drop Fields
DropFields_node1696463603921 = DropFields.apply(
    frame=Join_node1696463550611,
    paths=["timestamp"],
    transformation_ctx="DropFields_node1696463603921",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node2 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1696463603921,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://udacity.rugo.spark/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="MachineLearningCurated_node2",
)

job.commit()
