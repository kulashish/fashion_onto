CREATE EXTERNAL TABLE ${dbname}.appProductViews(
productsku      STRING,
totalCount      INT,
androidCount    STRING,
windowsCount    STRING,
iosCount        STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
LOCATION '${OUTPUT}/appProductViews';


INSERT OVERWRITE TABLE ${dbname}.appProductViews
SELECT tmp.productsku, sum(tmp.totalCount) as totalCount, sum(tmp.androidCount) as android,sum(tmp.windowsCount) as windows,sum(tmp.iosCount) as ios
FROM
  (
    SELECT productsku, IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'android', count(*), 0) androidCount,
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'windows', count(*), 0) windowsCount,
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'ios', count(*), 0) iosCount, count(*) totalCount
    FROM ${dbname}.${pagevisitTable} WHERE pagetype IN ('CPD') and domain IN ('android','windows','ios') and year1=${year1} and month1=${month1} and date1=${date1}
    GROUP BY productsku, REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1)
  ) tmp
GROUP BY tmp.productsku;


CREATE EXTERNAL TABLE ${dbname}.appProductImpressions(
impSku          STRING,
totalCount      INT,
androidCount    STRING,
windowsCount    STRING,
iosCount        STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
LOCATION '${OUTPUT}/appProductImpressions';


INSERT OVERWRITE TABLE ${dbname}.appProductImpressions
SELECT tmp.impSku, sum(tmp.totalCount) as totalCount, sum(tmp.androidCount) as android,sum(tmp.windowsCount) as windows,sum(tmp.iosCount) as ios
FROM
  (
    SELECT impSku, IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'android', count(*), 0) androidCount,
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'windows', count(*), 0) windowsCount,
      IF(REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1) == 'ios', count(*), 0) iosCount, count(*) totalCount
    FROM ${dbname}.${pagevisitTable}
    LATERAL VIEW EXPLODE(impressions.sku) imp AS impSku
    WHERE pagetype = 'CTL' and domain IN ('android','windows','ios') and year1=${year1} and month1=${month1} and date1=${date1}
    GROUP BY impSku, REGEXP_EXTRACT(mobileOs, '.*(android|windows|ios).*', 1)
  ) tmp
GROUP BY tmp.impSku;


CREATE EXTERNAL TABLE ${dbname}.desktopProductViews(
productsku      STRING,
totalCount      INT,
webCount        STRING,
mobileCount     STRING,
newFormulaCount STRING,
oldFormulaCount STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
LOCATION '${OUTPUT}/desktopProductViews';

INSERT OVERWRITE TABLE ${dbname}.desktopProductViews
SELECT tmp.productsku, sum(tmp.totalFormulaCount), sum(tmp.webCount),sum(tmp.mobileCount),sum(tmp.newFormulaCount), sum(tmp.oldFormulaCount)
FROM
  (
    SELECT productsku, IF(REGEXP_EXTRACT(formula, '.*(old|new).*', 1) == 'old', count(*), 0) oldFormulaCount,
      IF(REGEXP_EXTRACT(formula, '.*(old|new).*', 1) == 'new', count(*), 0) newFormulaCount,
      IF(domain == 'w', count(*), 0) webCount, IF(domain == 'm', count(*), 0) mobileCount, count(*) totalFormulaCount
    FROM ${dbname}.${pagevisitTable}
    WHERE pagetype IN ('QPD', 'CPD') and year1=${year1} and month1=${month1} and date1=${date1} and domain IN ('w','m')
    GROUP BY productsku, domain, REGEXP_EXTRACT(formula, '.*(old|new).*', 1)
  ) tmp
GROUP BY tmp.productsku;

CREATE EXTERNAL TABLE ${dbname}.desktopProductImpressions(
impSku          STRING,
totalCount      INT,
webCount        STRING,
mobileCount     STRING,
newFormulaCount STRING,
oldFormulaCount STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
LOCATION '${OUTPUT}/desktopProductImpressions';

INSERT OVERWRITE TABLE ${dbname}.desktopProductImpressions
SELECT tmp.impSku, sum(tmp.totalFormulaCount), sum(tmp.webCount),sum(tmp.mobileCount), sum(tmp.newFormulaCount), sum(tmp.oldFormulaCount)
FROM
  (
    SELECT impSku, IF(REGEXP_EXTRACT(formula, '.*(old|new).*', 1) == 'old', count(*), 0) oldFormulaCount,
    IF(REGEXP_EXTRACT(formula, '.*(old|new).*', 1) == 'new', count(*), 0) newFormulaCount,
      IF(domain == 'w', count(*), 0) webCount, IF(domain == 'm', count(*), 0) mobileCount, count(*) totalFormulaCount
    FROM ${dbname}.${pagevisitTable}
    LATERAL VIEW EXPLODE(impressions.sku) imp AS impSku
    WHERE pagetype = 'CTL' and year1=${year1} and month1=${month1} and date1=${date1} and domain IN ('w','m')
    GROUP BY impSku, domain, REGEXP_EXTRACT(formula, '.*(old|new).*', 1)
  ) tmp
GROUP BY tmp.impSku;

drop table ${dbname}.appProductViews;
drop table ${dbname}.appProductImpressions;
drop table ${dbname}.desktopProductViews;
drop table ${dbname}.desktopProductImpressions;
