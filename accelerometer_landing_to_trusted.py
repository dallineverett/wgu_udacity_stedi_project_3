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

# Script generated for node customer_trusted
customer_trusted_node1790208508729 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_trusted", transformation_ctx="customer_trusted_node1790208508729")

# Script generated for node accelerometer
accelerometer_node1790205431680 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="accelerometer_landing", transformation_ctx="accelerometer_node1790205431680")

# Script generated for node Accelerometer Landing to Trusted
SqlQuery1191 = '''
SELECT
accelerometer.*

FROM accelerometer AS accelerometer
    INNER JOIN customer_trusted AS customer_trusted ON accelerometer.user = customer_trusted.email
;
'''
AccelerometerLandingtoTrusted_node1790205481609 = sparkSqlQuery(glueContext, query = SqlQuery1191, mapping = {"accelerometer":accelerometer_node1790205431680, "customer_trusted":customer_trusted_node1790208508729}, transformation_ctx = "AccelerometerLandingtoTrusted_node1790205481609")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=AccelerometerLandingtoTrusted_node1790205481609, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790205192702", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790205687137 = glueContext.getSink(path="s3://data-goes-here-lake-house/accelerometer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1790205687137")
AmazonS3_node1790205687137.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="accelerometer_trusted")
AmazonS3_node1790205687137.setFormat("glueparquet", compression="snappy")
AmazonS3_node1790205687137.writeFrame(AccelerometerLandingtoTrusted_node1790205481609)
job.commit()