DROP DATABASE IF EXISTS ${hivedb} CASCADE;
CREATE DATABASE IF NOT EXISTS ${hivedb};

CREATE EXTERNAL TABLE ${hivedb}.productViews(
productsku      STRING,
totalCount      INT,
webCount        STRING,
mobileCount     STRING,
newFormulaCount STRING,
oldFormulaCount STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${FieldSeparator}
LOCATION '${OUTPUT}/productViews';

INSERT OVERWRITE TABLE ${hivedb}.productViews
SELECT tmp.productsku, sum(tmp.totalFormulaCount), sum(tmp.webCount),sum(tmp.mobileCount),sum(tmp.newFormulaCount), sum(tmp.oldFormulaCount) 
FROM
  (
    SELECT productsku, IF(REGEXP_EXTRACT(formula, '.*(old|new).*', 1) == 'old', count(*), 0) oldFormulaCount,
      IF(REGEXP_EXTRACT(formula, '.*(old|new).*', 1) == 'new', count(*), 0) newFormulaCount, 
      IF(domain == 'w', count(*), 0) webCount, IF(domain == 'm', count(*), 0) mobileCount, count(*) totalFormulaCount
    FROM ${pagevisitDB}.${pagevisitTable} WHERE pagetype IN ('QPD', 'CPD') and year1=${year1} and month1=${month1} and date1=${date1}
    GROUP BY productsku, domain, REGEXP_EXTRACT(formula, '.*(old|new).*', 1)
  ) tmp
GROUP BY tmp.productsku;

CREATE EXTERNAL TABLE ${hivedb}.productImpressions(
impSku          STRING,
totalCount      INT,
webCount        STRING,
mobileCount     STRING,
newFormulaCount STRING,
oldFormulaCount STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${FieldSeparator}
LOCATION '${OUTPUT}/productImpressions';

INSERT OVERWRITE TABLE ${hivedb}.productImpressions 
SELECT tmp.impSku, sum(tmp.totalFormulaCount), sum(tmp.webCount),sum(tmp.mobileCount), sum(tmp.newFormulaCount), sum(tmp.oldFormulaCount) 
FROM
  (
    SELECT impSku, IF(REGEXP_EXTRACT(formula, '.*(old|new).*', 1) == 'old', count(*), 0) oldFormulaCount,
    IF(REGEXP_EXTRACT(formula, '.*(old|new).*', 1) == 'new', count(*), 0) newFormulaCount,
      IF(domain == 'w', count(*), 0) webCount, IF(domain == 'm', count(*), 0) mobileCount, count(*) totalFormulaCount
    FROM ${pagevisitDB}.${pagevisitTable}
    LATERAL VIEW EXPLODE(impressions.sku) imp AS impSku 
    WHERE pagetype = 'CTL' and year1=${year1} and month1=${month1} and date1=${date1}
    GROUP BY impSku, domain, REGEXP_EXTRACT(formula, '.*(old|new).*', 1)
  ) tmp
GROUP BY tmp.impSku;
