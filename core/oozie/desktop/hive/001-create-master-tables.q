CREATE EXTERNAL TABLE ${hivedb}.browser( 
  browserid       STRING,
  useragent       STRING,
  ip              STRING,
  ts              TIMESTAMP
)
LOCATION '${INPUT}/browser'
TBLPROPERTIES ('serialization.null.format'=''); 

CREATE EXTERNAL TABLE ${hivedb}.visit( 
  visitid       STRING,
  browserid     STRING,
  ts            TIMESTAMP,
  domain        STRING,
  device        STRING
)
LOCATION '${INPUT}/visit'
TBLPROPERTIES ('serialization.null.format'=''); 


CREATE EXTERNAL TABLE ${hivedb}.visit_details( 
  id            STRING,
  visitid       STRING,
  userid        STRING,
  ts            TIMESTAMP
)
LOCATION '${INPUT}/visit_details'
TBLPROPERTIES ('serialization.null.format'=''); 


CREATE EXTERNAL TABLE ${hivedb}.page(
  id            STRING,
  visitid       STRING,
  channel       STRING,
  ts            TIMESTAMP,
  ip            STRING,
  categories    ARRAY<STRING>,
  url           STRING,
  pagetype      STRING,
  referrer      STRING,
  brand         STRING,
  sessionid     STRING,
  ajax          BOOLEAN,
  formula       STRING,
  productsku    STRING,
  position      STRUCT<srow: STRING, scol: STRING>,
  impressions   ARRAY<STRUCT<sku: STRING, srow: STRING, scol: STRING>>,
  matchscore    FLOAT,
  vouchercode   STRING,
  voucherfailedreason STRING,
  cartskus      ARRAY<STRING>,
  corder        STRUCT<id: STRING,totalprice:FLOAT,vouchercode:STRING,paymentmode:STRING, firstorder: BOOLEAN, products: ARRAY<STRUCT<sku: STRING, price: FLOAT>>>,
  suggestions   STRUCT<typedterm:STRING,clickedterm:STRING,rank:STRING,suggestionslist:ARRAY<STRING>>,
  bid STRING,
  actualvisitid STRING
  
)
LOCATION '${INPUT}/page'
TBLPROPERTIES ('serialization.null.format'='' ); 


CREATE TABLE ${hivedb}.visitbrowser 
AS
  SELECT v.browserid,
    v.visitid, 
    vd.userid,  
    v.ts,
    v.domain,
    v.device,
    b.useragent
  FROM ${hivedb}.visit v
  LEFT OUTER JOIN ${hivedb}.visit_details vd
  ON v.visitid = vd.visitid
  LEFT OUTER JOIN ${hivedb}.browser b 
  ON v.browserid = b.browserid
  WHERE (b.useragent!='EMPTY' AND LOWER(b.useragent) NOT LIKE '%wordpress%') OR b.useragent IS NULL;

