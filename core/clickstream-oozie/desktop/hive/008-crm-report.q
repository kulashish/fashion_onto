DROP DATABASE IF EXISTS ${hivedb} CASCADE;
CREATE DATABASE IF NOT EXISTS ${hivedb};


CREATE EXTERNAL TABLE ${hivedb}.surfProductViews(
userid     STRING,
visitid    STRING,
productsku STRING,
pagets     STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
LOCATION '${OUTPUT}/surfProductViews';

INSERT OVERWRITE TABLE ${hivedb}.surfProductViews
SELECT userid, visitid, productsku , pagets
FROM ${hivedb}.pagevisit 
WHERE productsku IS NOT NULL AND userid IS NOT NULL AND pagetype IN ('QPD', 'DPD', 'CPD')
SORT BY userid, pagets;

CREATE EXTERNAL TABLE ${hivedb}.surfCatalogViews(
userid     STRING,
visitid    STRING,
url        STRING,
pagets     STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
LOCATION '${OUTPUT}/surfCatalogViews';

INSERT OVERWRITE TABLE ${hivedb}.surfCatalogViews
SELECT userid, visitid, url , pagets
FROM ${hivedb}.pagevisit 
WHERE userid IS NOT NULL AND pagetype = 'CTL'
ORDER BY userid, pagets;

CREATE EXTERNAL TABLE ${hivedb}.pageViews(
userid     STRING,
productsku STRING,
pagets     STRING,
sessionid  STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
LOCATION '${OUTPUT}/pageViews';

INSERT OVERWRITE TABLE ${hivedb}.pageViews
SELECT userid, productsku,pagets,sessionid
FROM ${hivedb}.pagevisit
WHERE productsku IS NOT NULL AND userid IS NOT NULL AND pagetype ='CPD';

CREATE EXTERNAL TABLE ${hivedb}.lastCartStatus(
browserid  STRING,
userid     STRING,
cartskus   STRING,
pagets     STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
LOCATION '${OUTPUT}/lastCartStatus';

INSERT OVERWRITE TABLE ${hivedb}.lastCartStatus
SELECT a.browserid, a.userid, a.cartskus, a.pagets
FROM ${hivedb}.pagevisit a 
INNER JOIN (SELECT max(cast(pagets AS double)) AS ts, browserid FROM ${hivedb}.pagevisit WHERE pagetype='CRT' GROUP BY browserid) b
ON a.browserid=b.browserid AND cast(a.pagets AS double)=b.ts;


CREATE EXTERNAL TABLE ${hivedb}.voucherAttempts(
userid              STRING,
visitid             STRING,
pagets              STRING,
vouchercode         STRING,
voucherfailedreason STRING,
cartskus            STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
COLLECTION ITEMS TERMINATED BY ${hivevar:CollectionSeparator}
LOCATION '${OUTPUT}/voucherAttempts' ;

INSERT OVERWRITE TABLE ${hivedb}.voucherAttempts 
SELECT COALESCE(userid, '') as userid, visitid, pagets, vouchercode, voucherfailedreason, cartskus
FROM ${hivedb}.pagevisit
WHERE pagetype = 'CRV';

set hive.execution.engine=mr;

CREATE EXTERNAL TABLE ${hivedb}.searchLog(
userid         STRING,
visitid        STRING,
pagets         STRING,
search         STRING,
imp            STRING,
clickedterm    STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${hivevar:FieldSeparator}
COLLECTION ITEMS TERMINATED BY ${hivevar:CollectionSeparator}
LOCATION '${OUTPUT}/searchLog' ;

INSERT OVERWRITE TABLE ${hivedb}.searchLog 
SELECT a.userid, a.visitid, a.pagets, search, imp, COALESCE(b.clickedterm, '') as clickedterm
FROM
(
SELECT userid, visitid, pagets, REGEXP_EXTRACT(url, '.*\?qc?=([^&]+)(&|$)',1) search, COALESCE(impressions.sku, array()) imp
FROM ${hivedb}.pagevisit
WHERE userid IS NOT NULL AND pagetype = 'CTL' AND url RLIKE '.*\?qc?=.+' 
) a
JOIN
(
SELECT visitid, pagets, suggestions.clickedterm clickedterm FROM ${hivedb}.pagevisit WHERE suggestions IS NOT NULL
) b
ON a.visitid=b.visitid AND a.pagets=b.pagets;


