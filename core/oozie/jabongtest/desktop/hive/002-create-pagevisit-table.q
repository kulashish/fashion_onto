add jar reports-core-hive-udf.jar;

CREATE TEMPORARY FUNCTION deriveCategory AS 'com.jabong.report.hive.udf.CategoryUDF';


CREATE EXTERNAL TABLE ${hivedb}.pagevisit(
id                  STRING,
browserid           STRING,
visitid             STRING,
userid              STRING,
visitts             TIMESTAMP,
actualvisitid       STRING,
channel             STRING,
derivedcategory     STRING,
pagets              TIMESTAMP,
ip                  STRING,
categories          ARRAY<STRING>,
url                 STRING,
pagetype            STRING,
referrer            STRING,
brand               STRING,
sessionid           STRING,
ajax                BOOLEAN,
formula             STRING,
productsku          STRING,
impressions         ARRAY<STRUCT<sku: STRING, srow: STRING, scol: STRING>>,
matchscore          STRING,
vouchercode         STRING,
voucherfailedreason STRING,
cartskus            ARRAY<STRING>,
corder              STRUCT<id: STRING, totalprice: FLOAT, vouchercode: STRING, paymentmode: STRING, firstorder: BOOLEAN, products: ARRAY<STRUCT<sku: STRING, price: FLOAT>>>,
position            STRUCT<srow: STRING, scol: STRING>,
domain              STRING,
device              STRING,
suggestions         STRUCT<typedTerm: STRING, clickedTerm: STRING, rank: STRING, suggestionsList: ARRAY<STRING>>,
useragent           STRING
)
STORED AS ORC 
LOCATION '${PagevisitDir}'
TBLPROPERTIES ("orc.compress"="SNAPPY");

INSERT OVERWRITE TABLE ${hivedb}.pagevisit  
  SELECT /*+ STREAMTABLE(p) */p.id,
         v.browserid,
         v.visitid,
         v.userid,
         v.ts AS visitts,
         p.actualvisitid,
         p.channel,
         IF(p.pagetype == 'CTL', deriveCategory(p.categories, p.brand), COALESCE(s.category, 'Others')) AS derivedcategory,
         p.ts AS pagets,
         p.ip, 
         p.categories,
         p.url,
         p.pagetype,
         p.referrer,
         p.brand,
         p.sessionid,
         p.ajax,
         p.formula,
         p.productsku,
         p.impressions,
         p.matchscore,
         p.vouchercode,
         p.voucherfailedreason,
         p.cartskus,
         p.corder,
         p.position,
         v.domain,
         v.device,
         p.suggestions,
         v.useragent
  FROM ${hivedb}.visitbrowser v
  JOIN ${hivedb}.page p ON v.visitid = p.visitid
  LEFT OUTER JOIN ${hivedb}.SkuCategory s ON p.productsku = s.sku;
