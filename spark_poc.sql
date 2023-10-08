create database ankitdb;

create table asltab(name varchar, age int, city varchar);

insert into asltab 
values('venu', 43, 'hyd')
    ,('vamshi', 23, 'hyd')
    ,('venkat', 33, 'hyd')
    ,('vani', 35, 'hyd')
    ,('vegg', 43, 'hyd');

select * from asltab;

CREATE FILE FORMAT my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1;

DROP STAGE s3stage1;
  
CREATE STAGE s3stage
    URL='s3://sowmya97/input/'
    FILE_FORMAT=(FORMAT_NAME=my_csv_format)
    CREDENTIALS=(AWS_KEY_ID='AKIAX2Z235GDV4L5B4MY' AWS_SECRET_KEY='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');

list @s3stage;

copy into asltab from @s3stage 
files=('asl.csv')  FILE_FORMAT = (FORMAT_NAME = my_csv_format);

/*
from pyspark.sql import *
from pyspark import SparkConf, SparkContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark=SparkSession.builder.appName("test").master("local[*]").getOrCreate()


sfOptions = {
  "sfURL" : "rzftlse-px03616.snowflakecomputing.com",
  "sfUser" : "so34",
  "sfPassword" : "Hadoo",
  "sfDatabase" : "sowmyadb",
  "sfSchema" : "public",
  "sfWarehouse" : "COMPUTE_WH"
}

SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

df = spark.read.format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("dbtable",  "asltab") \
  .load()

#df.show()
res=df.where(col("city").isin("mas","blr","nlr"))
res.write.mode("append").format(SNOWFLAKE_SOURCE_NAME) \
  .options(**sfOptions) \
  .option("dbtable",  "aslmasblrnlr") \
  .save()
*/
