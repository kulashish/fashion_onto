ADD JAR /opt/artemisClickstream_pipeline/clickstream/code/lib/clickstream-core-hive-udf.jar;

CREATE TEMPORARY FUNCTION deriveCategory AS 'com.jabong.clickstream.hive.udf.CategoryUDF';
CREATE TEMPORARY FUNCTION replaceUnicode AS 'com.jabong.clickstream.hive.udf.DelimitUDF';

set hivevar:FieldSeparator='\;';
set hivevar:CollectionSeparator='|';
