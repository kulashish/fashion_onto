CREATE EXTERNAL TABLE ${hivedb}.productViews(
productsku      STRING,
totalCount      INT,
androidCount    STRING,
windowsCount    STRING,
iosCount        STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${FieldSeparator}
LOCATION '${OUTPUT}/appProductViews';


INSERT OVERWRITE TABLE ${hivedb}.productViews 
SELECT tmp.productsku, sum(tmp.totalCount) as totalCount, sum(tmp.androidCount) as android,sum(tmp.windowsCount) as windows,sum(tmp.iosCount) as ios
FROM
  (
    SELECT productsku, IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'android', count(*), 0) androidCount,
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'windows', count(*), 0) windowsCount, 
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'ios', count(*), 0) iosCount, count(*) totalCount
    FROM ${hivedb}.pagevisit WHERE pagetype IN ('CPD') 
    GROUP BY productsku, REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1)
  ) tmp
GROUP BY tmp.productsku;


CREATE EXTERNAL TABLE ${hivedb}.productImpressions(
impSku          STRING,
totalCount      INT,
androidCount    STRING,
windowsCount    STRING,
iosCount        STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${FieldSeparator}
LOCATION '${OUTPUT}/appProductImpressions';


INSERT OVERWRITE TABLE ${hivedb}.productImpressions 
SELECT tmp.impSku, sum(tmp.totalCount) as totalCount, sum(tmp.androidCount) as android,sum(tmp.windowsCount) as windows,sum(tmp.iosCount) as ios 
FROM
  (
    SELECT impSku, IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'android', count(*), 0) androidCount,
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'windows', count(*), 0) windowsCount, 
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'ios', count(*), 0) iosCount, count(*) totalCount
    FROM ${hivedb}.pagevisit
    LATERAL VIEW EXPLODE(impressions.sku) imp AS impSku WHERE pagetype = 'CTL'
    GROUP BY impSku, REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1)
  ) tmp
GROUP BY tmp.impSku;
