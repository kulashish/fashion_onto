CREATE EXTERNAL TABLE ${hivedb}.appStoreEmails(
deviceid        STRING,
appstoreemail   STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${FieldSeparator}
LOCATION '${OUTPUT}/appStoreEmails';

INSERT OVERWRITE TABLE ${hivedb}.appStoreEmails
SELECT bid as deviceid, appstoreemail
FROM ${hivedb}.pagevisit
WHERE appstoreemail IS NOT NULL;


CREATE EXTERNAL TABLE ${hivedb}.appPageViews(
userid        STRING,
productsku    STRING,
pageviewTime  STRING,
sessionid     STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ${FieldSeparator}
LOCATION '${OUTPUT}/appPageViews';

INSERT OVERWRITE TABLE ${hivedb}.appPageViews
SELECT userid, productsku,pagets,sessionid
FROM ${hivedb}.pagevisit
WHERE productsku IS NOT NULL AND userid IS NOT NULL AND pagetype ='CPD';
