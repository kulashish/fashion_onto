add jar reports-core-hive-udf.jar;

CREATE TEMPORARY FUNCTION deriveCategory AS 'com.jabong.report.hive.udf.CategoryUDF';

CREATE EXTERNAL TABLE ${hivedb}.SkuCategoryTmp( 
  sku            STRING,
  categories     ARRAY<STRING>
)
LOCATION '${S3_DAILY_INPUT}/sku'
TBLPROPERTIES ('serialization.null.format'=''); 


CREATE TABLE ${hivedb}.SkuCategory STORED AS ORC TBLPROPERTIES ("orc.compress"="SNAPPY")
AS
SELECT sku, deriveCategory(categories) AS category
FROM ${hivedb}.SkuCategoryTmp;
