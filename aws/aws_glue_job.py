###### UNIMIB - 2022 Indiegogo 
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job




##### FROM FILES
kickstarter_dataset_path = "s3://unimib-raw-data-2022/ds_project_details_full.csv"

###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()


glueContext = GlueContext(sc)
spark = glueContext.spark_session


    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


#### READ INPUT FILES TO CREATE AN INPUT DATASET
projects_dataset = spark.read \
    .option("header","true") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .csv(kickstarter_dataset_path)
    
projects_dataset.printSchema()

### REMOVE DUPLICATES
projects_dataset = projects_dataset.dropDuplicates(["project_id"])

#### FILTER ITEMS WITH NULL POSTING KEY
count_items = projects_dataset.count()
count_items_null = projects_dataset.filter("project_id is not null").count()

print(f"Number of items from RAW DATA {count_items}")
print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")



## READ TAGS DATASET
img_dataset_path = "s3://unimib-raw-data-2022/ds_img_details.csv"
img_dataset = spark.read.option("header","true").csv(img_dataset_path)



# CREATE THE AGGREGATE MODEL, ADD TAGS TO TEDX_DATASET
img_dataset_agg = img_dataset.groupBy(col("project_id").alias("project_id_ref")).agg(collect_list("name").alias("names"))
img_dataset_agg.printSchema()
projects_dataset_agg = projects_dataset.join(img_dataset_agg, projects_dataset.project_id == img_dataset_agg.project_id_ref, "left") \
    .drop("project_id_ref")

projects_dataset_agg.printSchema()

projects_dataset_agg.write.option("compression", "snappy").mode("overwrite").parquet("s3://unimib-dwh-2022/projects_dataset.out")
