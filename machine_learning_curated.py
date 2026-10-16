import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = glueContext.spark_session.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node accelerometer
accelerometer_node1791851705308 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_trusted", transformation_ctx="accelerometer_node1791851705308")

# Script generated for node step_trainer
step_trainer_node1791851703689 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_trusted", transformation_ctx="step_trainer_node1791851703689")

# Script generated for node SQL Query
SqlQuery453 = '''
SELECT
    step_trainer.sensorreadingtime,
    step_trainer.serialnumber,
    step_trainer.distancefromobject,
    accelerometer.user,
    accelerometer.x,
    accelerometer.y,
    accelerometer.z
    
FROM step_trainer
INNER JOIN accelerometer ON step_trainer.sensorreadingtime = accelerometer.timestamp
'''
SQLQuery_node1791851780092 = sparkSqlQuery(glueContext, query = SqlQuery453, mapping = {"step_trainer":step_trainer_node1791851703689, "accelerometer":accelerometer_node1791851705308}, transformation_ctx = "SQLQuery_node1791851780092")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1791851780092, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791847506698", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1791851871158 = glueContext.getSink(path="s3://data-goes-here-lake-house/machine_learning_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791851871158")
AmazonS3_node1791851871158.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="machine_learning_curated")
AmazonS3_node1791851871158.setFormat("glueparquet", compression="snappy")
AmazonS3_node1791851871158.writeFrame(SQLQuery_node1791851780092)
job.commit()