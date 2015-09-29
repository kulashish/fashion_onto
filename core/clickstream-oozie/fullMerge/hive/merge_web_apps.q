DROP TABLE IF EXISTS ${hivedb}.mergetemp;
CREATE EXTERNAL TABLE ${hivedb}.mergetemp(
id                  string,
browserid           string,
visitid             string,
userid              string,
actualvisitid       string,
channel             string,
derivedcategory     string,
pagets              timestamp,
ip                  string,
categories          array<string>,
url                 string,
pagetype            string,
referrer            string,
brand               string,
sessionid           string,
ajax                boolean,
formula             string,
productsku          string,
impressions         array<struct<sku:string,srow:string,scol:string>>,
vouchercode         string,
voucherfailedreason string,
cartskus            array<string>,
corder              struct<id:string,totalprice:float,vouchercode:string,paymentmode:string,firstorder:boolean,products:array<struct<sku:string,price:float>>>,
position            struct<srow:string,scol:string>,
domain              string,
device              string,
suggestions         struct<typedterm:string,clickedterm:string,rank:string,suggestionslist:array<string>>,
useragent           string,
visitts             timestamp,
usermatch           string,
appstoreemail       string,
internetconnection  string,
mobileos            string,
installdate         string,
appversion          string,
loginmode           string,
add4push            string
)
STORED AS ORC 
LOCATION '${MergeDir}'
TBLPROPERTIES ('serialization.null.format'='' );

INSERT OVERWRITE TABLE ${hivedb}.mergetemp  
SELECT id, browserid, visitid, userid, actualvisitid, channel, derivedcategory, pagets, ip, categories, url, pagetype, referrer, brand, sessionid, ajax, formula, productsku, impressions, vouchercode, voucherfailedreason, cartskus, corder, position, domain, device, suggestions, useragent, visitts, usermatch, appstoreemail, internetconnection, mobileos, installdate, appversion, loginmode, add4push
FROM
(
SELECT id,
      bid as browserid,
      visitid,
      userid,
      actualvisitid,
      channel,
      derivedcategory,
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
      Null as visitts,
      userMatch,
      appStoreEmail,
      internetConnection,
      mobileOs,
      installDate,
      appVersion,
      loginMode,
      add4Push
FROM ${hivedb}.${appsTable} WHERE date1=${date1} and month1=${month1} and year1=${year1} 

UNION ALL

SELECT id,
    browserid,
    visitid,
    userid,
    actualvisitid,
    channel,
    derivedcategory,
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
    visitts,
    Null AS usermatch,
    Null AS appstoreemail,
    Null AS internetconnection,
    Null AS mobileos,
    Null AS installdate,
    Null AS appversion,
    Null AS loginmode,
    Null AS add4push
FROM ${hivedb}.${DesktopMergeTable} WHERE date1=${date1} and month1=${month1} and year1=${year1} 
) unioned;

DROP TABLE ${hivedb}.mergeTemp;
