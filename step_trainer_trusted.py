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

# Script generated for node customers
customers_node1791850733455 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customers_curated", transformation_ctx="customers_node1791850733455")

# Script generated for node step_trainer
step_trainer_node1791850731442 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="step_trainer_landing", transformation_ctx="step_trainer_node1791850731442")

# Script generated for node SQL Query
SqlQuery533 = '''
SELECT 
step_trainer.*

FROM step_trainer
JOIN customers ON step_trainer.serialnumber = customers.serialnumber
'''
SQLQuery_node1791850796697 = sparkSqlQuery(glueContext, query = SqlQuery533, mapping = {"step_trainer":step_trainer_node1791850731442, "customers":customers_node1791850733455}, transformation_ctx = "SQLQuery_node1791850796697")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1791850796697, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791847506698", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1791850871489 = glueContext.getSink(path="s3://data-goes-here-lake-house/step_trainer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791850871489")
AmazonS3_node1791850871489.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="step_trainer_trusted")
AmazonS3_node1791850871489.setFormat("glueparquet", compression="snappy")
AmazonS3_node1791850871489.writeFrame(SQLQuery_node1791850796697)
job.commit()