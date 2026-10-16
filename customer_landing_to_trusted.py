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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1790205431680 = glueContext.create_dynamic_frame.from_catalog(database="stedi_project", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1790205431680")

# Script generated for node Customer Landing to Trusted
SqlQuery266 = '''
SELECT 
*

FROM myDataSource

WHERE TRUE 
-- filters out records where this is null, only want those who consented to participate
-- in the research
AND sharewithresearchasofdate IS NOT NULL
;
'''
CustomerLandingtoTrusted_node1790205481609 = sparkSqlQuery(glueContext, query = SqlQuery266, mapping = {"myDataSource":AWSGlueDataCatalog_node1790205431680}, transformation_ctx = "CustomerLandingtoTrusted_node1790205481609")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=CustomerLandingtoTrusted_node1790205481609, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1790205192702", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1790205687137 = glueContext.getSink(path="s3://data-goes-here-lake-house/customer_trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1790205687137")
AmazonS3_node1790205687137.setCatalogInfo(catalogDatabase="stedi_project",catalogTableName="customer_trusted")
AmazonS3_node1790205687137.setFormat("glueparquet", compression="snappy")
AmazonS3_node1790205687137.writeFrame(CustomerLandingtoTrusted_node1790205481609)
job.commit()