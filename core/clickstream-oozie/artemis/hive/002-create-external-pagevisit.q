
CREATE EXTERNAL TABLE ${hivedb}.pagevisit(
id                 STRING,
bid                STRING,
userid             STRING,
channel            STRING,
pagets             TIMESTAMP,
ip                 STRING,
categories         ARRAY<STRING>,
url                STRING,
pagetype           STRING,
referrer           STRING,
brand              STRING,
sessionid          STRING,
ajax               BOOLEAN,
formula            STRING,
productsku         STRING,
impressions        ARRAY<STRUCT<sku: STRING, srow: STRING, scol: STRING>>,
vouchercode        STRING,
voucherfailedreason STRING,
cartskus           ARRAY<STRING>,
corder             STRUCT<id: STRING, totalprice: FLOAT, vouchercode: STRING, paymentmode: STRING, firstorder: BOOLEAN, products: ARRAY<STRUCT<sku: STRING, price: FLOAT>>>,
position           STRUCT<srow: STRING, scol: STRING>,
domain             STRING,
device             STRING,
suggestions        STRUCT<typedTerm: STRING, clickedTerm: STRING, rank: STRING, suggestionsList: ARRAY<STRING>>,
useragent          STRING,
userMatch          STRING,
appStoreEmail      STRING,
internetConnection STRING,
mobileOs           STRING,
visitid            STRING,
actualvisitid      STRING,
derivedcategory    STRING,
installDate        STRING,
appVersion         STRING,
loginMode          STRING,
add4Push           STRING

)
STORED AS ORC 
LOCATION '${PagevisitDir}'
TBLPROPERTIES ("orc.compress"="SNAPPY"); 



INSERT OVERWRITE TABLE ${hivedb}.pagevisit
   SELECT id,
      bid,
      userid,
      channel,
      pagets,
      ip,
      categories,
      url,
      pagetype,
      referrer,
      brand,
      sessionid,
      ajax,
      formula,
      productsku,
      impressions,
      vouchercode,
      voucherfailedreason,
      cartskus,
      corder,
      position,
      domain,
      device,
      suggestions,
      useragent,
      userMatch,
      appStoreEmail,
      internetConnection,
      mobileOs,
      visitid,
      actualvisitid,
      derivedcategory,
      installDate,
      appVersion,
      loginMode,
      add4Push
  FROM ${hivedb}.pageview;

