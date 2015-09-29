set hive.optimize.reducededuplication = false;

CREATE EXTERNAL TABLE ${dbname}.productViews(
productsku      STRING,
totalCount      INT,
androidCount    STRING,
windowsCount    STRING,
iosCount        STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
LOCATION '${OUTPUT}/appProductViews';


INSERT OVERWRITE TABLE ${dbname}.productViews 
SELECT tmp.productsku, sum(tmp.totalCount) as totalCount, sum(tmp.androidCount) as android,sum(tmp.windowsCount) as windows,sum(tmp.iosCount) as ios
FROM
  (
    SELECT productsku, IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'android', count(*), 0) androidCount,
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'windows', count(*), 0) windowsCount, 
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'ios', count(*), 0) iosCount, count(*) totalCount
    FROM ${pagevisitDB}.${pagevisitTable} WHERE pagetype IN ('CPD') and domain IN ('android','windows','ios')
    GROUP BY productsku, REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1)
  ) tmp
GROUP BY tmp.productsku;


CREATE EXTERNAL TABLE ${dbname}.productImpressions(
impSku          STRING,
totalCount      INT,
androidCount    STRING,
windowsCount    STRING,
iosCount        STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
LOCATION '${OUTPUT}/appProductImpressions';


INSERT OVERWRITE TABLE ${dbname}.productImpressions 
SELECT tmp.impSku, sum(tmp.totalCount) as totalCount, sum(tmp.androidCount) as android,sum(tmp.windowsCount) as windows,sum(tmp.iosCount) as ios 
FROM
  (
    SELECT impSku, IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'android', count(*), 0) androidCount,
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'windows', count(*), 0) windowsCount, 
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'ios', count(*), 0) iosCount, count(*) totalCount
    FROM ${pagevisitDB}.${pagevisitTable}
    LATERAL VIEW EXPLODE(impressions.sku) imp AS impSku 
    WHERE pagetype = 'CTL' and domain IN ('android','windows','ios')
    GROUP BY impSku, REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1)
  ) tmp
GROUP BY tmp.impSku;
