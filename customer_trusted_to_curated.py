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

# Script generated for node customer
customer_node1791847524253 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_trusted", transformation_ctx="customer_node1791847524253")

# Script generated for node accelerometer
accelerometer_node1791847521157 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_trusted", transformation_ctx="accelerometer_node1791847521157")

# Script generated for node SQL Query
SqlQuery411 = '''
SELECT 
-- using distinct because the relationship between customers and 
-- readings is 1:m so distinct removes duplicate customers from the
-- resulting join
DISTINCT
customer.*

FROM customer
    INNER JOIN accelerometer ON customer.email = accelerometer.user 


'''
SQLQuery_node1791847634957 = sparkSqlQuery(glueContext, query = SqlQuery411, mapping = {"customer":customer_node1791847524253, "accelerometer":accelerometer_node1791847521157}, transformation_ctx = "SQLQuery_node1791847634957")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=SQLQuery_node1791847634957, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1791847506698", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1791847842837 = glueContext.getSink(path="s3://data-goes-here-lake-house/customers_curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1791847842837")
AmazonS3_node1791847842837.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customers_curated")
AmazonS3_node1791847842837.setFormat("glueparquet", compression="snappy")
AmazonS3_node1791847842837.writeFrame(SQLQuery_node1791847634957)
job.commit()